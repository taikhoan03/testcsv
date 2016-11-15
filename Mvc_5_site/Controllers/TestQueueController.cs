using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;

namespace Mvc_5_site.Controllers
{
    public class TestQueueController : Controller
    {
        //[HttpGet]
        //[AsyncTimeout(8000)]
        // GET: TestQueue
        
        public async Task<ActionResult> Index()
        {
            var task = new Task(() => {
                //lay du lieu - first priority tu db

                FastPriorityQueueExample.RunExample();
            });
            ///*Task T=*/FastPriorityQueueExample.RunExample();
            task.Start();
            return View();
        }
        //[HttpGet]
        //[AsyncTimeout(8000)]
        public async Task<ActionResult> add()
        {
            //var t=FastPriorityQueueExample._addToQueue("ghjfdglkf", 0);
            //var T=FastPriorityQueueExample.priorityQueue.Enqueue(new QueueData("jfldskjflkd"), 0);
            //t.Start();
            var task2 = new Task(() => {
                FastPriorityQueueExample._addToQueue("ghjfdglkf", 0);
            });
            task2.Start();
            return View();
        }
    }
}