using System.Net.Sockets;

namespace Fireflies.Atlas.Distributed.Server;

public sealed class AsyncUserToken : IDisposable {
    public Socket Socket { get; }
    public int? MessageSize { get; set; }
    public int DataStartOffset { get; set; }
    public int NextReceiveOffset { get; set; }

    public AsyncUserToken(Socket socket) {
        Socket = socket;
    }

    #region IDisposable Members

    public void Dispose() {
        try {
            Socket.Shutdown(SocketShutdown.Send);
        } catch (Exception) {
        }

        try {
            Socket.Close();
        } catch (Exception) {
        }
    }

    #endregion
}