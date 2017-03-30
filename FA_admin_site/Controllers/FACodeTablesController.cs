using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using System.Net;
using System.Web;
using System.Web.Mvc;
using BL;

namespace FA_admin_site.Controllers
{
    public class FACodeTablesController : Controller
    {
        private DA_Model db = new DA_Model();

        // GET: FACodeTables
        public async Task<ActionResult> Index()
        {
            return View(await db.FACodeTables.ToListAsync());
        }

        // GET: FACodeTables/Details/5
        public async Task<ActionResult> Details(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            FACodeTable fACodeTable = await db.FACodeTables.FindAsync(id);
            if (fACodeTable == null)
            {
                return HttpNotFound();
            }
            return View(fACodeTable);
        }

        // GET: FACodeTables/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: FACodeTables/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Create([Bind(Include = "Id,County,TableName")] FACodeTable fACodeTable)
        {
            if (ModelState.IsValid)
            {
                db.FACodeTables.Add(fACodeTable);
                await db.SaveChangesAsync();
                return RedirectToAction("Index");
            }

            return View(fACodeTable);
        }

        // GET: FACodeTables/Edit/5
        public async Task<ActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            FACodeTable fACodeTable = await db.FACodeTables.FindAsync(id);
            if (fACodeTable == null)
            {
                return HttpNotFound();
            }
            return View(fACodeTable);
        }

        // POST: FACodeTables/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> Edit([Bind(Include = "Id,County,TableName")] FACodeTable fACodeTable)
        {
            if (ModelState.IsValid)
            {
                db.Entry(fACodeTable).State = EntityState.Modified;
                await db.SaveChangesAsync();
                return RedirectToAction("Index");
            }
            return View(fACodeTable);
        }

        // GET: FACodeTables/Delete/5
        public async Task<ActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            FACodeTable fACodeTable = await db.FACodeTables.FindAsync(id);
            if (fACodeTable == null)
            {
                return HttpNotFound();
            }
            return View(fACodeTable);
        }

        // POST: FACodeTables/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> DeleteConfirmed(int id)
        {
            FACodeTable fACodeTable = await db.FACodeTables.FindAsync(id);
            db.FACodeTables.Remove(fACodeTable);
            await db.SaveChangesAsync();
            return RedirectToAction("Index");
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }
    }
}
