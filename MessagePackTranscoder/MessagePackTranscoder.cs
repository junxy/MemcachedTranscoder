﻿using Enyim.Caching.Memcached;
using MsgPack.Serialization;
using System;
using System.Collections.Concurrent;
using System.IO;

namespace MemcachedTranscoder
{
    public class MessagePackTranscoder : DefaultTranscoder
    {
        private static readonly ConcurrentDictionary<string, Type> readCache = new ConcurrentDictionary<string, Type>();
        private static readonly ConcurrentDictionary<Type, string> writeCache = new ConcurrentDictionary<Type, string>();
        private static readonly SerializationContext defaultContext = new SerializationContext();

        protected override object DeserializeObject(ArraySegment<byte> value)
        {
            using (var ms = new MemoryStream(value.Array, value.Offset, value.Count, writable: false))
            {
                var unpacker = MsgPack.Unpacker.Create(ms);

                // unpack object
                unpacker.Read();
                if (unpacker.IsArrayHeader)
                {
                    // read type
                    unpacker.Read();
                    var typeName = (string)unpacker.Data;
                    var type = readCache.GetOrAdd(typeName, x => Type.GetType(x, throwOnError: true)); // Get type or Register type

                    // unpack object
                    unpacker.Read();
                    var unpackedValue = MessagePackSerializer.Create(type, defaultContext).UnpackFrom(unpacker);

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
                MessagePackSerializer.Create(type, defaultContext).PackTo(packer, value);

                return new ArraySegment<byte>(ms.ToArray(), 0, (int)ms.Length);
            }
        }
    }
}