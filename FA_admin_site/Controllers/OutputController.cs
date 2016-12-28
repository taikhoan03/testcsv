using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FA_admin_site.Controllers
{
    public class OutputController : Controller
    {
        // GET: Output
        public ActionResult Index()
        {
            var db = new BL.DA_Model();
            var outputFields = db.outputFields.OrderBy(p=>p.Order);
            ViewBag.OutputFields = JsonConvert.SerializeObject(outputFields);
            return View(db.outputMappers);
        }
        public ActionResult Map(int id)
        {
            var db = new BL.DA_Model();
            var outputFields = db.outputFields.OrderBy(p => p.Order);
            ViewBag.OutputFields = JsonConvert.SerializeObject(outputFields);
            return View(db.outputMappers);
        }
        [HttpPost]
        public void CreateNewOutput(BL.OutputMapper file)
        {
            var db = new BL.DA_Model();
            db.outputMappers.Add(file);
            db.SaveChanges();
        }
        [HttpPost]
        public void CreateNewField(BL.OutputFields field)
        {
            var db = new BL.DA_Model();
            var newOrder = db.outputFields.Where(p => p.OutputMapperId == field.OutputMapperId).Max(p => p.Order);
            field.Order = newOrder + 1;
            db.outputFields.Add(field);
            db.SaveChanges();
        }
        [HttpPost]
        public void del(int id)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id==id);
            db.outputFields.Remove(field);
            db.SaveChanges();
        }
        [HttpPost]
        public void re_order(List<BL.OutputFields> fields)
        {
            var new_order = 0;
            var db = new BL.DA_Model();
            var OutputMapperId = fields.First().OutputMapperId;
            var dbfields = db.outputFields.Where(p => p.OutputMapperId == OutputMapperId);
            foreach (var f in fields)
            {
                new_order++;
                var dbfield = dbfields.FirstOrDefault(p => p.Id == f.Id);
                dbfield.Order = new_order;
            }
            db.SaveChanges();
        }
        public void UpdateFieldName(int pk,string value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Name = value;
            db.SaveChanges();
        }
        public void UpdateFieldType(int pk, string value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Type = value;
            db.SaveChanges();
        }
        public void UpdateFieldLength(int pk, int value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Length = value;
            db.SaveChanges();
        }
        public void UpdateFieldDecimal(int pk, int value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Decimal = value;
            db.SaveChanges();
        }
        
    }
}