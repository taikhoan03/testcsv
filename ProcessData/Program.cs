using Priority_Queue;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BL;
using System.Threading;
using System.IO;

namespace ProcessData
{
    class Program
    {
        static int MAX_USERS_IN_QUEUE = 10, sleepTime=5;
        static bool F_refresh = false;
        static string F_refresh_key = "reset.fa";
        static FastPriorityQueue<ProcessFile> priorityQueue = new FastPriorityQueue<ProcessFile>(MAX_USERS_IN_QUEUE);
        static void Main(string[] args)
        {
            watch();

           var db = new DA_Model();
            while (true)
            {
                Console.WriteLine("Running process");
                //tao du lieu queue
                if (priorityQueue.Count == 0)
                {
                    priorityQueue.Enqueue(new ProcessFile(), 1);
                    priorityQueue.Enqueue(new ProcessFile(), 2);
                    priorityQueue.Enqueue(new ProcessFile(), 3);
                    //TODO: update db queueing
                }
                //chay queue
                while (priorityQueue.Count != 0)
                {
                    if (F_refresh)
                    {
                        Console.WriteLine("Refreshed...");
                        priorityQueue.Clear();
                        F_refresh = false;
                        break;
                    }
                    priorityQueue.Dequeue();
                    //TODO: update db status -> processing
                    Console.WriteLine(priorityQueue.Count);
                    //TODO: update db status -> done/fail
                    //kiem tra co flag reset, neu co se tao lai queue
                    //priorityQueue.Clear();
                }
                Console.WriteLine("Process done, sleeping...");
                Thread.Sleep(sleepTime * 1000);
            }
        }
        private static void watch()
        {
            FileSystemWatcher watcher = new FileSystemWatcher();
            watcher.Path = @"D:\FA_in_out\Tmp_data";
            //watcher.NotifyFilter = NotifyFilters.CreationTime;
            watcher.NotifyFilter = NotifyFilters.LastWrite;
            watcher.Filter = "*.fa";
            watcher.Changed += new FileSystemEventHandler(OnChanged);
            watcher.EnableRaisingEvents = true;
        }
        private static void OnChanged(object source, FileSystemEventArgs e)
        {
            if (e.Name == F_refresh_key)
            {
                //priorityQueue.Clear();
                F_refresh = true;
            }
            
            
        }
        static void getData(FastPriorityQueue<ProcessFile> priorityQueue)
        {

        }
    }
}
