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
            var outputFields = db.FACodes.OrderBy(p => p.Order);
            ViewBag.OutputFields = JsonConvert.SerializeObject(outputFields);
            ViewBag.Id = id;
            return View();
        }
        [HttpPost]
        public void CreateNewCode(BL.FACode code)
        {
            using (var db = new BL.DA_Model())
            {
                code.IsEditable = false;
                code.Order = 0;// db.FACodes.Where(p => p.TableId == code.TableId).Max(p => p.Order) + 1;
                code.Createdby = System.Web.HttpContext.Current.User.Identity.Name;
                db.FACodes.Add(code);
                db.SaveChanges();
            }
        }
    }
}