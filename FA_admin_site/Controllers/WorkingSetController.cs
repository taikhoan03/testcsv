using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Libs;
using System.Threading.Tasks;
using System.Net;
using BL;

namespace FA_admin_site.Controllers
{
    [Authorize]
    public class WorkingSetController : Controller
    {
        BL.DA_Model db = new BL.DA_Model();
        // GET: WorkingSet
        public ActionResult Index()
        {
            

            var packages = db.packages.Where(p=>p.Createdby== System.Web.HttpContext.Current.User.Identity.Name);
            
            ViewBag.packages = packages.ToList();
            //ViewBag.jsfiles = Json(allFiles);
            return View();
        }
        public ActionResult Merge()
        {


            var ws = db.workingSets.Where(p=>p.User== System.Web.HttpContext.Current.User.Identity.Name);

            ViewBag.ws = ws;
            //ViewBag.jsfiles = Json(allFiles);
            return View();
        }
        public ActionResult Manage(int? id)
        {


            var ws = db.workingSets.Where(p => p.User == System.Web.HttpContext.Current.User.Identity.Name);

            ViewBag.ws = ws;
            if (id == null)
            {
                id = -1;
            }
            ViewBag.ID = id;
            //ViewBag.jsfiles = Json(allFiles);
            return View();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id">wokingSetId</param>
        /// <returns></returns>
        public ActionResult Layout(int id)
        {
            var allFiles = db.workingSetItems.Where(p => p.WorkingSetId == id);
            var wsFile = db.workingSets.FirstOrDefault(p => p.Id == id);
            ViewBag.WorkingSetInfo = wsFile;
            //ViewBag.Filename = wsiFile.Filename;
            return View(allFiles);
        }
        public ActionResult Layout2(int id)
        {
            var allFiles = db.workingSetItems.Where(p => p.WorkingSetId == id);
            var wsFile = db.workingSets.FirstOrDefault(p => p.Id == id);
            ViewBag.WorkingSetInfo = wsFile;
            //ViewBag.Filename = wsiFile.Filename;
            return View(allFiles);
        }
        [HttpGet]
        public JsonResult GetLayout(int fileid)
        {
            var columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == fileid);
            if(columns.Count()==0)
            {
                using (var client = new System.Net.WebClient())
                {
                    //var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetFileInfo?state=" + job.State + "&county=" + job.County + "&filename=" + job.Filename);
                    var json = client.DownloadString(Url.Action("Index", "JobLayout", null, this.Request.Url.Scheme) + "/?id=" + fileid);
                }
            }
            columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == fileid);
            return Json(columns, JsonRequestBehavior.AllowGet);
        }
        [HttpGet]
        public void SavePrimaryKey(int workingSetItemId,string primaryKey)
        {
            var item = db.workingSetItems.FirstOrDefault(p => p.Id == workingSetItemId);
            if (item != null)
            {
                item.PrimaryKey = primaryKey.ReplaceUnusedCharacters();
                db.SaveChanges();
            }
        }
        [HttpGet]
        public void ChangeFieldType(int workingSetItemId, int type)
        {
            var item = db.jobFileLayouts.FirstOrDefault(p => p.Id == workingSetItemId);
            if (item != null)
            {
                item.Type = type;
                db.SaveChanges();
            }
        }
        [HttpGet]
        public JsonResult workingSetItem(int packid,bool checkPrimary=true)
        {
            //set session
            var sess = System.Web.HttpContext.Current.Session[EV.KEY_UserSession];
            if (sess == null)
            {
                var user = new BL.UserData();
                user.Username = System.Web.HttpContext.Current.User.Identity.Name;
                user.Current_wsid = packid;
                System.Web.HttpContext.Current.Session[EV.KEY_UserSession] = user;
            }
            else
            {
                var user = (BL.UserData)sess;
                user.Username = System.Web.HttpContext.Current.User.Identity.Name;
                user.Current_wsid = packid;
            }

            var allFiles = db.workingSetItems.Where(p => p.WorkingSetId == packid && !p.IsMerged);
            // chỉ lấy những file đã dc layout (có Primary key)
            if(checkPrimary)
                allFiles = allFiles.Where(p => !string.IsNullOrEmpty(p.PrimaryKey)); //allFiles.FirstOrDefault().Id
            return Json(allFiles, JsonRequestBehavior.AllowGet);
        }
        [HttpGet]
        public JsonResult allFiles(int packid)
        {
            var allFiles = db.files.Where(p=>p.Packageid==packid);
            return Json(allFiles,JsonRequestBehavior.AllowGet);
        }
        public class WorkingSetData
        {
            public BL.WorkingSet ws { get; set; }
            public BL.WorkingSetItem[] items { get; set; }
        }
        [HttpPost]
        public void create(WorkingSetData wd)
        {
            var db = new BL.DA_Model();
            using (var dbContextTransaction = db.Database.BeginTransaction())
            {
                try
                {
                    var date_create = DateTime.Now;
                    //tạo package
                    var workingSet = new BL.WorkingSet();
                    workingSet.County = wd.ws.County;
                    workingSet.State = wd.ws.State;
                    workingSet.User = System.Web.HttpContext.Current.User.Identity.Name;
                    workingSet.Createdate = date_create;
                    workingSet.Edition = wd.ws.Edition;
                    workingSet.Version = wd.ws.Version;
                    //package.Status = "Processing";
                    db.workingSets.Add(workingSet);
                    db.SaveChanges();
                    //throw new Exception("fksljd");
                    //tạo các files thuộc về package
                    foreach (var item in wd.items)
                    {
                        var wsItem = new BL.WorkingSetItem();
                        //file.County = json.County;
                        //file.Create_date = date_create;
                        wsItem.Filename = item.Filename;
                        wsItem.PrimaryKey = item.PrimaryKey.ReplaceUnusedCharacters();
                        wsItem.SecondaryKeys = item.SecondaryKeys;
                        wsItem.WorkingSetId = workingSet.Id;
                        //file. = "test";
                        db.workingSetItems.Add(wsItem);
                    }


                    db.SaveChanges();
                    dbContextTransaction.Commit();
                    //return;// RedirectToAction("Index");
                    //db.Entry(movie).State = EntityState.Modified;
                    //db.SaveChanges();

                }
                catch (Exception exT)
                {
                    dbContextTransaction.Rollback();
                    ModelState.AddModelError("", exT.InnerException.Message);
                    throw exT;
                }


            }
        }
        [HttpPost]
        public void postMerge(Merge_info mergeInfo)
        {
            //first insert to db
            var db = new BL.DA_Model();
            var ws = db.workingSets.Where(p => p.Id == mergeInfo.wsId).FirstOrDefault();
            var _mergeJob = new BL.MergeFileJob();
            _mergeJob.County = ws.County;
            _mergeJob.Filenames = string.Join(",", mergeInfo.filenames);
            var now = DateTime.Now;
            _mergeJob.Finishdate = now;
            foreach (var item in mergeInfo.details)
            {
                item.RenameTo = item.RenameTo.Trim().ReplaceUnusedCharacters();
            }
            _mergeJob.MergeDetails = mergeInfo.details.XmlSerialize();
            _mergeJob.OutputFilename = mergeInfo.output_filename;
            _mergeJob.Runby = System.Web.HttpContext.Current.User.Identity.Name;
            _mergeJob.Rundate = now;
            _mergeJob.State = ws.State;
            _mergeJob.WorkingJobId = ws.Id;
            
            db.mergeFileJob.Add(_mergeJob);
            db.SaveChanges();
            if (mergeInfo.runAfterSave)
            {
                var savedId = _mergeJob.Id;
                var url = Config.Get_local_control_site() + "/JSON/MergeFiles/?id=" + savedId+ "&primaryKey="+mergeInfo.primaryKey;
                runMergeFromControlSite(url);
            }
        }
        public ActionResult MergeJobs()
        {
            var db = new BL.DA_Model();
            return View(db.mergeFileJob);
        }
        public async Task<string> runMergeFromControlSite(string url)
        {
            using (WebClient webClient = new WebClient())
            {
                return webClient.DownloadString(url);
            }
            return "";
        }
        public class Merge_info
        {
            //public string state { get; set; }
            //public string county { get; set; }
            public int wsId { get; set; }
            public string desc { get; set; }
            public List<string> filenames { get; set; }
            public List<BL.MergeDetail> details { get; set; }
            public string output_filename { get; set; }
            public string primaryKey { get; set; }
            public bool runAfterSave { get; set; }

        }
    }
}