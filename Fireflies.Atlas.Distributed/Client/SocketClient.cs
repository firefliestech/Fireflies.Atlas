using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Fireflies.Atlas.Distributed.Client;

public abstract class SocketClient : IDisposable {
    private readonly int _bufferSize = 6000000;
    private const int MessageHeaderSize = 4;

    private readonly Socket _clientSocket;
    private readonly IPEndPoint _hostEndPoint;
    private readonly AutoResetEvent _autoConnectEvent;
    private readonly AutoResetEvent _autoSendEvent;
    private readonly SocketAsyncEventArgs _sendEventArgs;
    private readonly SocketAsyncEventArgs _receiveEventArgs;
    private readonly BlockingCollection<byte[]> _sendingQueue;
    private readonly BlockingCollection<byte[]> _receivedMessageQueue;
    private readonly Thread _sendMessageWorker;
    private readonly Thread _processReceivedMessageWorker;

    protected SocketClient(IPEndPoint hostEndPoint) {
        _hostEndPoint = hostEndPoint;
        _autoConnectEvent = new AutoResetEvent(false);
        _autoSendEvent = new AutoResetEvent(false);
        _sendingQueue = new BlockingCollection<byte[]>();
        _receivedMessageQueue = new BlockingCollection<byte[]>();
        _clientSocket = new Socket(_hostEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        _sendMessageWorker = new Thread(SendQueueMessage);
        _processReceivedMessageWorker = new Thread(ProcessReceivedMessage);

        _sendEventArgs = new SocketAsyncEventArgs();
        _sendEventArgs.UserToken = _clientSocket;
        _sendEventArgs.RemoteEndPoint = _hostEndPoint;
        _sendEventArgs.Completed += OnSend;

        _receiveEventArgs = new SocketAsyncEventArgs();
        _receiveEventArgs.UserToken = new AsyncUserToken(_clientSocket);
        _receiveEventArgs.RemoteEndPoint = _hostEndPoint;
        _receiveEventArgs.SetBuffer(new byte[_bufferSize], 0, _bufferSize);
        _receiveEventArgs.Completed += OnReceive;
    }

    public virtual void Connect() {
        var connectArgs = new SocketAsyncEventArgs();
        connectArgs.UserToken = _clientSocket;
        connectArgs.RemoteEndPoint = _hostEndPoint;
        connectArgs.Completed += OnConnect;

        _clientSocket.ConnectAsync(connectArgs);
        _autoConnectEvent.WaitOne();

        var errorCode = connectArgs.SocketError;
        if (errorCode != SocketError.Success) {
            throw new SocketException((int)errorCode);
        }

        _sendMessageWorker.Start();
        _processReceivedMessageWorker.Start();

        if (!_clientSocket.ReceiveAsync(_receiveEventArgs)) {
            ProcessReceive(_receiveEventArgs);
        }
    }

    public void Disconnect() {
        _clientSocket.Disconnect(false);
    }

    public void Send(byte[] message) {
        _sendingQueue.Add(message);
    }

    private void OnConnect(object sender, SocketAsyncEventArgs e) {
        _autoConnectEvent.Set();
    }

    private void OnSend(object sender, SocketAsyncEventArgs e) {
        _autoSendEvent.Set();
    }

    private void SendQueueMessage() {
        while (true) {
            var message = _sendingQueue.Take();
            if (message != null) {
                var withHeader = BuildMessage(message);
                _sendEventArgs.SetBuffer(withHeader, 0, withHeader.Length);
                if(_clientSocket.SendAsync(_sendEventArgs))
                    _autoSendEvent.WaitOne();
            }
        }
    }

    private void OnReceive(object sender, SocketAsyncEventArgs e) {
        ProcessReceive(e);
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
            ProcessError(e);
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
                _receivedMessageQueue.Add(messageData);

                token.DataStartOffset = dataStartOffset + messageSize;
                token.MessageSize = null;

                ProcessReceivedData(token.DataStartOffset, totalReceivedDataSize, alreadyProcessedDataSize + messageSize, token, e);
            } else {
            }
        }
    }

    static byte[] BuildMessage(byte[] data) {
        var header = BitConverter.GetBytes(data.Length);
        var message = new byte[header.Length + data.Length];
        header.CopyTo(message, 0);
        data.CopyTo(message, header.Length);
        return message;
    }

    private async void ProcessReceivedMessage() {
        while (true) {
            var message = _receivedMessageQueue.Take();
            if (message != null) {
                await ProcessMessage(message);
                //var current = Interlocked.Increment(ref Program._receivedMessageCount);
                //if (current == 1) {
                //    Program._watch = Stopwatch.StartNew();
                //}

                //if (current % 1000 == 0) {
                //    Console.WriteLine("received reply message, length:{0}, count:{1}, timeSpent:{2}", message.Length, current, Program._watch.ElapsedMilliseconds);
                //}
            }
        }
    }

    protected abstract Task ProcessMessage(byte[] message);

    private void ProcessError(SocketAsyncEventArgs e) {
        //Socket s = e.UserToken as Socket;
        //if (s.Connected)
        //{
        //    // close the socket associated with the client
        //    try
        //    {
        //        s.Shutdown(SocketShutdown.Both);
        //    }
        //    catch (Exception)
        //    {
        //        // throws if client process has already closed
        //    }
        //    finally
        //    {
        //        if (s.Connected)
        //        {
        //            s.Close();
        //        }
        //    }
        //}

        // Throw the SocketException
        throw new SocketException((Int32)e.SocketError);
    }

    #region IDisposable Members

    public void Dispose() {
        _autoConnectEvent.Close();
        if (_clientSocket.Connected) {
            _clientSocket.Close();
        }
    }

    #endregion
}