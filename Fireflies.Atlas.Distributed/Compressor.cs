using System.Buffers;
using System.IO.Compression;

namespace Fireflies.Atlas.Distributed;

public static class Compressor {
    public static byte[] Compress(byte[] bytes) {
        using var to = new MemoryStream();
        using var gZipStream = new GZipStream(to, CompressionMode.Compress);
        gZipStream.Write(bytes, 0, bytes.Length);
        gZipStream.Flush();
        return to.ToArray();
    }

    public static byte[] Compress(Stream stream) {
        stream.Position = 0;

        using var to = new MemoryStream();
        using var gZipStream = new GZipStream(to, CompressionMode.Compress);
        stream.CopyTo(gZipStream);
        gZipStream.Flush();
        return to.ToArray();
    }

    public static byte[] Decompress(byte[] compressed) {
        using var from = new MemoryStream(compressed);
        using var to = new MemoryStream();
        using var gZipStream = new GZipStream(from, CompressionMode.Decompress);
        gZipStream.CopyTo(to);
        return to.ToArray();
    }

    public static byte[] Decompress(Stream from) {
        using var to = new MemoryStream();
        using var gZipStream = new GZipStream(from, CompressionMode.Decompress);
        gZipStream.CopyTo(to);
        return to.ToArray();
    }

    public static byte[] Decompress(ReadOnlySequence<byte> compressed) {
        using var ms = new MemoryStream();
        foreach (var mem in compressed) {
            ms.Write(mem.Span);
        }

        ms.Position = 0;
        return Decompress(ms);
    }
}