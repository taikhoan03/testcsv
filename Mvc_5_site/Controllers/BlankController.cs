using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Mvc_5_site.Controllers
{
    public class BlankController : Controller
    {
        // GET: Blank
        public ActionResult Index()
        {
            return View();
        }

        // GET: Blank/Details/5
        public ActionResult Details(int id)
        {
            return View();
        }

        // GET: Blank/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Blank/Create
        [HttpPost]
        public ActionResult Create(FormCollection collection)
        {
            try
            {

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        // GET: Blank/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: Blank/Edit/5
        [HttpPost]
        public ActionResult Edit(int id, FormCollection collection)
        {
            try
            {

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        // GET: Blank/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: Blank/Delete/5
        [HttpPost]
        public ActionResult Delete(int id, FormCollection collection)
        {
            try
            {

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }
    }
}
