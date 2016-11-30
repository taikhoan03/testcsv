using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;
using BL;
namespace FA_admin_site.Controllers
{
    public class JobLayoutController : Controller
    {
        // GET: JobLayout
        public ActionResult Index(int id)
        {
            ViewBag.WorkingSetItemId = id;
            var db = new BL.DA_Model();
            var wsiFile = db.workingSetItems.Find(id);
            var wsFile = db.workingSets.FirstOrDefault(p => p.Id == wsiFile.WorkingSetId);
            ViewBag.WorkingSetInfo = wsFile;
            ViewBag.Filename = wsiFile.Filename;
            var columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == id);
            List<BL.JobFileLayout> rs = new List<BL.JobFileLayout>();
            //If no data => create default from headers
            if (columns.Count() == 0)
            {
                
                using (var client = new System.Net.WebClient())
                {
                    //var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetFileInfo?state=" + job.State + "&county=" + job.County + "&filename=" + job.Filename);
                    var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetHeader?state="+wsFile.State+"&county="+wsFile.County+"&filename="+ wsiFile.Filename);
                    var headers = new System.Web.Script.Serialization.JavaScriptSerializer().Deserialize<string[]>(json);
                    var order = 1;
                    foreach (var header in headers)
                    {
                        var column = new BL.JobFileLayout
                        {
                            WorkingSetItemId = id,
                            Fieldname = header.ReplaceUnusedCharacters(),
                            Mapper = "{" + header.ReplaceUnusedCharacters() + "}",
                            Order = order,
                            Type=1
                        };
                        order++;
                        db.jobFileLayouts.Add(column);
                        rs.Add(column);
                    }
                    db.SaveChanges();
                }
            }else
            {
                rs = db.jobFileLayouts.Where(p => p.WorkingSetItemId == id).ToList();
            }
            return View(rs);
        }
        [HttpPost]
        public void ResetColumns(int id) {
            var db = new BL.DA_Model();
            var wsiFile = db.workingSetItems.Find(id);
            //var wsiFile = db.workingSetItems.Find(id);
            var wsFile = db.workingSets.FirstOrDefault(p => p.Id == wsiFile.WorkingSetId);
            var columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == id);

            using (var dbContextTransaction = db.Database.BeginTransaction())
            {
                try
                {
                    foreach (var col in columns)
                    {
                        db.jobFileLayouts.Remove(col);
                    }
                    db.SaveChanges();
                    using (var client = new System.Net.WebClient())
                    {
                        //var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetFileInfo?state=" + job.State + "&county=" + job.County + "&filename=" + job.Filename);
                        var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetHeader?state="+wsFile.State+"&county="+wsFile.County+"&filename="+wsiFile.Filename);
                        var headers = new System.Web.Script.Serialization.JavaScriptSerializer().Deserialize<string[]>(json);
                        var order = 1;
                        foreach (var header in headers)
                        {
                            var column = new BL.JobFileLayout
                            {
                                WorkingSetItemId = id,
                                Fieldname = header,
                                Mapper = "{" + header + "}",
                                Order = order
                            };
                            order++;
                            db.jobFileLayouts.Add(column);
                        }
                        db.SaveChanges();

                    }
                    dbContextTransaction.Commit();
                }
                catch (Exception)
                {
                    dbContextTransaction.Rollback();
                }
            }

        }
        [HttpPost]
        public void NewField(string name,string value,int order)
        {
            var db = new BL.DA_Model();
            var newfield = new BL.JobFileLayout();
            newfield.Fieldname = name;
            newfield.Mapper = value;
            newfield.Order = order;
            newfield.WorkingSetItemId = 1;
            db.jobFileLayouts.Add(newfield);
            db.SaveChanges();
        }
        [HttpPost]
        public void DelField(int id)
        {
            var db = new BL.DA_Model();
            var field = db.jobFileLayouts.Find(id);
            db.jobFileLayouts.Remove(field);
            db.SaveChanges();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id">workingSetItemid</param>
        /// <param name="filename"></param>
        /// <returns></returns>
        public JsonResult GetTestLayoutResult(int id, string filename)
        {
            var db            = new BL.DA_Model();
            var wsiFile       = db.workingSetItems.Find(id);
            var wsFile        = db.workingSets.FirstOrDefault(p => p.Id == wsiFile.WorkingSetId);
            var columns       = db.jobFileLayouts.Where(p => p.WorkingSetItemId == id);
            using (var client = new System.Net.WebClient())
            {
                //var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetFileInfo?state=" + job.State + "&county=" + job.County + "&filename=" + job.Filename);
                var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetTestLayoutResult?workingSetItemid="+id+"&state=" + wsFile.State+"&county="+wsFile.County+"&filename="+wsiFile.Filename);
                if(json.StartsWith("Error Note") || json.StartsWith("\"Error Note"))
                {
                    throw new Exception(HttpUtility.HtmlDecode(System.Text.RegularExpressions.Regex.Unescape(json)));
                }
                    
                var headers = new System.Web.Script.Serialization.JavaScriptSerializer().Deserialize<List<string[]>>(json);
                
                return Json(headers, JsonRequestBehavior.AllowGet);
            }
            
        }
        public async Task<ActionResult> testAsync(string url)
        {
            ViewBag.SyncOrAsync = "Asynchronous";
            var gizmoService = new GizmoService();
            //ViewBag.NumberOfRows = await gizmoService.GetGizmosAsync(url);//wait
            ViewBag.NumberOfRows = gizmoService.GetGizmosAsync(url);//wait
            return View("testAsync");
        }
    }

    public class GizmoService
    {
        public async Task<string> GetGizmosAsync(string url)
        {
            return GetGizmos(url);
        }
        // Implementation removed.

        public string GetGizmos(string url)
        {
            //var uri = Util.getServiceUri("Gizmos");
            using (WebClient webClient = new WebClient())
            {
                return webClient.DownloadString(url);
            }
        }
    }
}