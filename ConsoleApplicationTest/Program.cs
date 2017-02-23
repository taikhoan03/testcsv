using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplicationTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //var watch = Stopwatch.StartNew();
            //var b = Helpers.ReadCSV.Read("XF_1.txt", @"D:\FA_in_out\XF_1.txt", int.MaxValue);
            var a = Helpers.ReadCSV.ReadAsDictionary("XF_1.txt", @"D:\FA_in_out\XF_1.txt", int.MaxValue);

        }
    }
}
