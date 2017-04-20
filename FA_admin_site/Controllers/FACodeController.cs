using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FA_admin_site.Controllers
{
    public class FACodeController : Controller
    {
        // GET: FACode
        public ActionResult Index(int id)
        {
            var db = new BL.DA_Model();
            var outputFields = db.FACodes.Where(p=>p.TableId==id).OrderBy(p => p.Order);
            var FACodeTable = db.FACodeTables.Find(id);
            ViewBag.OutputFields = JsonConvert.SerializeObject(outputFields);
            ViewBag.Id = id;
            ViewBag.TableName = FACodeTable.TableNameID;
            return View();
        }
        [HttpPost]
        [ValidateInput(false)]
        public void CreateNewCode(BL.FACode code)
        {
            using (var db = new BL.DA_Model())
            {
                //var found = db.FACodes.Any(p => p.LookupValue == code.LookupValue && p.TableId == code.TableId);
                //if (found)
                //    throw new Exception("This code has been existed");

                code.IsEditable = false;
                code.Order = 0;// db.FACodes.Where(p => p.TableId == code.TableId).Max(p => p.Order) + 1;
                code.Createdby = System.Web.HttpContext.Current.User.Identity.Name;
                if (string.IsNullOrEmpty(code.Comment))
                    code.Comment = ".";
                db.FACodes.Add(code);
                db.SaveChanges();
            }
        }
        [HttpPost]
        public void Update(int pk, string fieldname,string value)
        {
            using (var db = new BL.DA_Model())
            {
                var item = db.FACodes.FirstOrDefault(p => p.Id == pk);
                if (fieldname == "code")
                {
                    item.Code = value;
                }else if (fieldname=="lkData")
                {
                    item.LookupValue = value;
                }
                else if (fieldname == "cmt")
                {
                    item.Comment = value;
                }
                db.SaveChanges();
            }
            
        }
    }
}