using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Priority_Queue;
namespace TestQueue1
{
    class Program
    {
        static void Main(string[] args)
        {
        }
        public static class SimplePriorityQueueExample
        {
            private const int MAX_USERS_IN_QUEUE = 10000;
            public static FastPriorityQueue<QueueData> priorityQueue = new FastPriorityQueue<QueueData>(MAX_USERS_IN_QUEUE);
            public static void RunExample()
            {
                //First, we create the priority queue.
                /// lay du lieu tu db

                //Now, let's add them all to the queue (in some arbitrary order)!
                priorityQueue.Enqueue(new QueueData("4"), 4);
                priorityQueue.Enqueue(new QueueData("0"), 0); //Note: Priority = 0 right now!
                priorityQueue.Enqueue(new QueueData("1"), 1);
                priorityQueue.Enqueue(new QueueData("4"), 4);
                priorityQueue.Enqueue(new QueueData("3"), 3);

                //Change one of the string's priority to 2.  Since this string is already in the priority queue, we call UpdatePriority() to do this
                //priorityQueue.UpdatePriority("2 - Tyler", 2);

                //Finally, we'll dequeue all the strings and print them out
                while (priorityQueue.Count != 0)
                {
                    var nextUser = priorityQueue.Dequeue();
                    Console.WriteLine(nextUser.Name);
                    System.Threading.Thread.Sleep(2000);
                }

                //Output:
                //1 - Jason
                //2 - Tyler
                //3 - Valerie
                //4 - Joseph
                //4 - Ryan

                //Notice that when two strings with the same priority were enqueued, they were dequeued in the same order that they were enqueued.
            }
            public static void addToQueue(string item,float priority)
            {
                priorityQueue.Enqueue(new QueueData(item), priority);

            }
            //public static void removeFromQueue()
            //{
            //    if
            //}
        }
    }
    public class QueueData : FastPriorityQueueNode
    {
        public string Name { get; set; }
        public QueueData(string name)
        {
            Name = name;
        }
    }
}
