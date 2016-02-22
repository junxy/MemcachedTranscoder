﻿using Enyim.Caching.Memcached;
using MsgPack.Serialization;
using ProtoBuf;
using System;
using System.Diagnostics;
using System.Linq;

namespace MemcachedTranscoder.PerfTest
{
    [ProtoContract]
    [Serializable]
    public class TestClass
    {
        [ProtoMember(1)]
        [MessagePackMember(0)]
        public string MyProperty1 { get; set; }

        [ProtoMember(2)]
        [MessagePackMember(1)]
        public int MyProperty2 { get; set; }

        [ProtoMember(3)]
        [MessagePackMember(2)]
        public DateTime MyProperty3 { get; set; }

        [ProtoMember(4)]
        [MessagePackMember(3)]
        public bool MyProperty4 { get; set; }

        public override bool Equals(object obj)
        {
            var tc = (TestClass)obj;
            return this.MyProperty1 == tc.MyProperty1
                && this.MyProperty2 == tc.MyProperty2
                && this.MyProperty3 == tc.MyProperty3
                && this.MyProperty4 == tc.MyProperty4;
        }
    }

    internal class Program
    {
        private static void Bench(object data, ITranscoder transcoder, int repeat)
        {
            // warmup and copy
            var item = transcoder.Serialize(data);
            var ___ = transcoder.Deserialize(item);

            var comparer = System.Collections.StructuralComparisons.StructuralEqualityComparer;
            if (!comparer.Equals(data, ___)) throw new Exception("failed");

            var items = Enumerable.Range(0, repeat).Select(_ =>
            {
                return new CacheItem(item.Flags,
                    new ArraySegment<byte>(item.Data.Array.ToArray(), item.Data.Offset, item.Data.Count));
            }).ToArray();

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var sw = Stopwatch.StartNew();

            for (int i = 0; i < repeat; i++)
            {
                var _ = transcoder.Serialize(data);
            }

            sw.Stop();
            Console.WriteLine("S " + transcoder.GetType().Name + ":" + (int)sw.Elapsed.TotalMilliseconds + " ms");
            sw.Restart();

            foreach (var x in items)
            {
                var _ = transcoder.Deserialize(x);
            }

            sw.Stop();
            Console.WriteLine("D " + transcoder.GetType().Name + ":" + (int)sw.Elapsed.TotalMilliseconds + " ms");
            Console.WriteLine("Size:" + item.Data.Count);
        }

        private static void Main(string[] args)
        {
            var obj = new TestClass
            {
                MyProperty1 = "hoge",
                MyProperty2 = 1,
                MyProperty3 = new DateTime(1999, 12, 11, 0, 0, 0, DateTimeKind.Utc),
                MyProperty4 = true
            };

            var array = Enumerable.Range(1, 10)
                .Select(i => new TestClass
                {
                    MyProperty1 = "hoge" + i,
                    MyProperty2 = i,
                    MyProperty3 = new DateTime(1999, 12, 11, 0, 0, 0, DateTimeKind.Utc).AddDays(i),
                    MyProperty4 = i % 2 == 0
                })
                .ToArray();

            Console.WriteLine("Simple POCO************************");

            var count = 100000;

            Bench(obj, new DefaultTranscoder(), count);
            Bench(obj, new DataContractTranscoder(), count);
            Bench(obj, new ProtoTranscoder(), count);
            Bench(obj, new JsonTranscoder(), count);
            Bench(obj, new MessagePackTranscoder(), count);
            Bench(obj, new MessagePackGzipTranscoder(), count);
            Bench(obj, new MessagePackMapTranscoder(), count);

            Console.WriteLine("Array******************************");

            Bench(array, new DefaultTranscoder(), count);
            Bench(array, new DataContractTranscoder(), count);
            Bench(array, new ProtoTranscoder(), count);
            Bench(array, new JsonTranscoder(), count);
            Bench(array, new MessagePackTranscoder(), count);
            Bench(array, new MessagePackGzipTranscoder(), count);
            Bench(array, new MessagePackMapTranscoder(), count);
        }
    }
}