using Enyim.Caching.Memcached;
using MsgPack.Serialization;
using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Compression;

namespace MemcachedTranscoder
{
    public class MessagePackGzipTranscoder : DefaultTranscoder
    {
        static readonly ConcurrentDictionary<string, Type> readCache = new ConcurrentDictionary<string, Type>();
        static readonly ConcurrentDictionary<Type, string> writeCache = new ConcurrentDictionary<Type, string>();
        static readonly SerializationContext defaultContext = new SerializationContext();

        // via: http://stackoverflow.com/questions/7013771/decompress-byte-array-to-string-via-binaryreader-yields-empty-string 
        private static byte[] Compress(byte[] data)
        {
            using (var compressedStream = new MemoryStream())
//            using (var zipStream = new GZipOutputStream(compressedStream))
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Compress))
            {
                zipStream.Write(data, 0, data.Length);
                zipStream.Close();
                return compressedStream.ToArray();
            }
        }

        private static byte[] Decompress(byte[] data)
        {
            using (var compressedStream = new MemoryStream(data))
//            using (var zipStream = new GZipInputStream(compressedStream))
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (var resultStream = new MemoryStream())
            {
                zipStream.CopyTo(resultStream);
                return resultStream.ToArray();
            }
        }


        protected override object DeserializeObject(ArraySegment<byte> value)
        {
            var data = Decompress(value.Array);

            using (var ms = new MemoryStream(data))
            {
                var unpacker = MsgPack.Unpacker.Create(ms);

                // unpack object
                unpacker.Read();
                if (unpacker.IsArrayHeader)
                {
                    // read type
                    unpacker.Read();
                    var typeName = (string) unpacker.Data;
                    var type = readCache.GetOrAdd(typeName, x => Type.GetType(x, throwOnError: true));
                    // Get type or Register type

                    // unpack object
                    unpacker.Read();

                    var unpackedValue = MessagePackSerializer.Get(type, defaultContext).UnpackFrom(unpacker);
//                    var unpackedValue = defaultContext.GetSerializer(type).Unpack(ms);

                    return unpackedValue;
                }
                else
                {
                    throw new InvalidDataException("MessagePackTranscoder only supports [\"TypeName\", object]");
                }
            }
        }

        protected override ArraySegment<byte> SerializeObject(object value)
        {
            var type = value.GetType();
            var typeName = writeCache.GetOrAdd(type, TypeHelper.BuildTypeName); // Get type or Register type

            using (var ms = new MemoryStream())
            {
                var packer = MsgPack.Packer.Create(ms);

                packer.PackArrayHeader(2); // ["type", obj]

                packer.PackString(typeName); // Pack Type

                // Pack Object
                MessagePackSerializer.Get(type, defaultContext).PackTo(packer, value);
                var data = ms.ToArray();
                var data2 = Compress(data);
                return new ArraySegment<byte>(data2, 0, data2.Length);
            }
        }
    }
}