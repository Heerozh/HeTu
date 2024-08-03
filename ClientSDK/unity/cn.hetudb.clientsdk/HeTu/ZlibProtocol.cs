#if HETU_CLIENT_USING_ZLIB

using Unity.SharpZipLib.Zip.Compression.Streams;
using System.IO;

namespace HeTu
{
    /// <summary>
    /// 启用zlib压缩需要在Project Settings->Player->Other Settings->
    /// Scripting Define Symbols中添加 HETU_CLIENT_USING_ZLIB
    /// 然后去Package Manager手动安装Unity.SharpZipLib包
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

#endif
