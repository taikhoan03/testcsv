using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;

namespace FA_admin_site.Controllers
{
    public class DCController : Controller
    {
        // GET: DC
        public ActionResult Index()
        {
            return View();
        }
        public ActionResult Select()
        {
            
            return View("SelectStateCountyForImport");
        }
        public ActionResult ManageDataImport()
        {
            var db = new BL.DA_Model();
            var rs = db.packages;
            return View(rs);
        }
    }
}