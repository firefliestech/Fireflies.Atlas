using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Net;

namespace Fireflies.Atlas.Distributed.Server;

public abstract class SocketListener {
    private const int MessageHeaderSize = 4;
    private int _receivedMessageCount = 0; //for testing
    private Stopwatch _watch; //for testing

    private readonly BlockingCollection<MessageData> _sendingQueue;
    private readonly Thread _sendMessageWorker;

    private static readonly Mutex _mutex = new Mutex();
    private Socket _listenSocket;
    private readonly int _bufferSize;
    private int _connectedSocketCount;
    private readonly int _maxConnectionCount;
    private readonly SocketAsyncEventArgsPool _socketAsyncReceiveEventArgsPool;
    private readonly SocketAsyncEventArgsPool _socketAsyncSendEventArgsPool;
    private readonly Semaphore _acceptedClientsSemaphore;
    private readonly AutoResetEvent _waitSendEvent;

    public SocketListener(int maxConnectionCount, int bufferSize) {
        _maxConnectionCount = maxConnectionCount;
        _bufferSize = bufferSize;
        _socketAsyncReceiveEventArgsPool = new SocketAsyncEventArgsPool(maxConnectionCount);
        _socketAsyncSendEventArgsPool = new SocketAsyncEventArgsPool(maxConnectionCount);
        _acceptedClientsSemaphore = new Semaphore(maxConnectionCount, maxConnectionCount);

        _sendingQueue = new BlockingCollection<MessageData>();
        _sendMessageWorker = new Thread(SendQueueMessage);

        for (var i = 0; i < maxConnectionCount; i++) {
            var socketAsyncEventArgs = new SocketAsyncEventArgs();
            socketAsyncEventArgs.Completed += OnIOCompleted;
            socketAsyncEventArgs.SetBuffer(new byte[bufferSize], 0, bufferSize);
            _socketAsyncReceiveEventArgsPool.Push(socketAsyncEventArgs);
        }

        for (var i = 0; i < maxConnectionCount; i++) {
            var socketAsyncEventArgs = new SocketAsyncEventArgs();
            socketAsyncEventArgs.Completed += OnIOCompleted;
            socketAsyncEventArgs.SetBuffer(new byte[bufferSize], 0, bufferSize);
            _socketAsyncSendEventArgsPool.Push(socketAsyncEventArgs);
        }

        _waitSendEvent = new AutoResetEvent(false);
    }

    public void Start(IPEndPoint localEndPoint) {
        _listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _listenSocket.ReceiveBufferSize = _bufferSize;
        _listenSocket.SendBufferSize = _bufferSize;
        _listenSocket.Bind(localEndPoint);
        _listenSocket.Listen(_maxConnectionCount);
        _sendMessageWorker.Start();
        StartAccept(null);
        _mutex.WaitOne();
    }

    public void Stop() {
        try {
            _listenSocket.Close();
        } catch {
        }

        _mutex.ReleaseMutex();
    }


    public void Send(byte[] message, AsyncUserToken token) {
        _sendingQueue.Add(new MessageData { Message = message, Token = token });
    }

    private void OnIOCompleted(object sender, SocketAsyncEventArgs e) {
        switch (e.LastOperation) {
            case SocketAsyncOperation.Receive:
                ProcessReceive(e);
                break;
            case SocketAsyncOperation.Send:
                ProcessSend(e);
                break;
            default:
                throw new ArgumentException("The last operation completed on the socket was not a receive or send");
        }
    }

    private void StartAccept(SocketAsyncEventArgs acceptEventArg) {
        if (acceptEventArg == null) {
            acceptEventArg = new SocketAsyncEventArgs();
            acceptEventArg.Completed += (sender, e) => ProcessAccept(e);
        } else {
            acceptEventArg.AcceptSocket = null;
        }

        _acceptedClientsSemaphore.WaitOne();
        if (!_listenSocket.AcceptAsync(acceptEventArg)) {
            ProcessAccept(acceptEventArg);
        }
    }

    private void ProcessAccept(SocketAsyncEventArgs e) {
        try {
            var readEventArgs = _socketAsyncReceiveEventArgsPool.Pop();
            if (readEventArgs != null) {
                readEventArgs.UserToken = new AsyncUserToken(e.AcceptSocket);
                Interlocked.Increment(ref _connectedSocketCount);
                Console.WriteLine("Client connection accepted. There are {0} clients connected to the server", _connectedSocketCount);
                if (!e.AcceptSocket.ReceiveAsync(readEventArgs)) {
                    ProcessReceive(readEventArgs);
                }
            } else {
                Console.WriteLine("There are no more available sockets to allocate.");
            }
        } catch (SocketException ex) {
            var token = e.UserToken as AsyncUserToken;
            Console.WriteLine("Error when processing data received from {0}:\r\n{1}", token.Socket.RemoteEndPoint, ex.ToString());
        } catch (Exception ex) {
            Console.WriteLine(ex.ToString());
        }

        // Accept the next connection request.
        StartAccept(e);
    }

    private void ProcessReceive(SocketAsyncEventArgs e) {
        if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success) {
            var token = e.UserToken as AsyncUserToken;

            ProcessReceivedData(token.DataStartOffset, token.NextReceiveOffset - token.DataStartOffset + e.BytesTransferred, 0, token, e);

            token.NextReceiveOffset += e.BytesTransferred;

            if (token.NextReceiveOffset == e.Buffer.Length) {
                token.NextReceiveOffset = 0;

                if (token.DataStartOffset < e.Buffer.Length) {
                    var notYesProcessDataSize = e.Buffer.Length - token.DataStartOffset;
                    Buffer.BlockCopy(e.Buffer, token.DataStartOffset, e.Buffer, 0, notYesProcessDataSize);

                    token.NextReceiveOffset = notYesProcessDataSize;
                }

                token.DataStartOffset = 0;
            }

            e.SetBuffer(token.NextReceiveOffset, e.Buffer.Length - token.NextReceiveOffset);

            if (!token.Socket.ReceiveAsync(e)) {
                ProcessReceive(e);
            }
        } else {
            CloseClientSocket(e);
        }
    }

    private void ProcessReceivedData(int dataStartOffset, int totalReceivedDataSize, int alreadyProcessedDataSize, AsyncUserToken token, SocketAsyncEventArgs e) {
        if (alreadyProcessedDataSize >= totalReceivedDataSize) {
            return;
        }

        if (token.MessageSize == null) {
            if (totalReceivedDataSize > MessageHeaderSize) {
                var headerData = new byte[MessageHeaderSize];
                Buffer.BlockCopy(e.Buffer, dataStartOffset, headerData, 0, MessageHeaderSize);
                var messageSize = BitConverter.ToInt32(headerData, 0);

                token.MessageSize = messageSize;
                token.DataStartOffset = dataStartOffset + MessageHeaderSize;

                ProcessReceivedData(token.DataStartOffset, totalReceivedDataSize, alreadyProcessedDataSize + MessageHeaderSize, token, e);
            } else {
            }
        } else {
            var messageSize = token.MessageSize.Value;

            if (totalReceivedDataSize - alreadyProcessedDataSize >= messageSize) {
                var messageData = new byte[messageSize];
                Buffer.BlockCopy(e.Buffer, dataStartOffset, messageData, 0, messageSize);
                ProcessMessage(messageData, token, e);


                token.DataStartOffset = dataStartOffset + messageSize;
                token.MessageSize = null;


                ProcessReceivedData(token.DataStartOffset, totalReceivedDataSize, alreadyProcessedDataSize + messageSize, token, e);
            } else {
            }
        }
    }

    protected abstract void ProcessMessage(byte[] messageData, AsyncUserToken token, SocketAsyncEventArgs e);

    private void ProcessSend(SocketAsyncEventArgs e) {
        _socketAsyncSendEventArgsPool.Push(e);
        _waitSendEvent.Set();
    }

    private void SendQueueMessage() {
        while (true) {
            var messageData = _sendingQueue.Take();
            SendMessage(messageData, BuildMessage(messageData.Message));
        }
    }

    private void SendMessage(MessageData messageData, byte[] message) {
        var sendEventArgs = _socketAsyncSendEventArgsPool.Pop();
        if (sendEventArgs != null) {
            sendEventArgs.SetBuffer(message, 0, message.Length);
            sendEventArgs.UserToken = messageData.Token;
            messageData.Token.Socket.SendAsync(sendEventArgs);
        } else {
            _waitSendEvent.WaitOne();
            SendMessage(messageData, message);
        }
    }

    static byte[] BuildMessage(byte[] data) {
        var header = BitConverter.GetBytes(data.Length);
        var message = new byte[header.Length + data.Length];
        header.CopyTo(message, 0);
        data.CopyTo(message, header.Length);
        return message;
    }

    private void CloseClientSocket(SocketAsyncEventArgs e) {
        var token = e.UserToken as AsyncUserToken;
        token.Dispose();
        _acceptedClientsSemaphore.Release();
        Interlocked.Decrement(ref _connectedSocketCount);
        Console.WriteLine("A client has been disconnected from the server. There are {0} clients connected to the server", _connectedSocketCount);
        _socketAsyncReceiveEventArgsPool.Push(e);
    }
}