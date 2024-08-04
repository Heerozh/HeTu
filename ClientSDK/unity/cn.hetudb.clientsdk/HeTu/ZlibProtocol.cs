using Unity.SharpZipLib.Zip.Compression.Streams;
using System.IO;

namespace HeTu
{
    /// <summary>
    /// 默认的消息压缩协议
    /// </summary>
    public class ZlibProtocol: IProtocol
    {
        public byte[] Compress(byte[] data)
        {
            var zipBuff = new MemoryStream();
            var zipStream = new DeflaterOutputStream(zipBuff);
            var input = new MemoryStream(data);
            input.CopyTo(zipStream);
            zipStream.Finish();
            return zipBuff.ToArray();
        }

        public byte[] Decompress(byte[] data)
        {
            var unzipBuff = new MemoryStream();
            var input = new MemoryStream(data);
            var unzipStream = new InflaterInputStream(input);
            unzipStream.CopyTo(unzipBuff);
            return unzipBuff.ToArray();
        }

    }
}

