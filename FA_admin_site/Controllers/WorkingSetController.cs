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
        //[HttpPost]
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id">workingSetId</param>
        /// <returns></returns>
        public ActionResult TransferColumnsToRecord(int id)
        {
            var db = new BL.DA_Model();
            var files = db.workingSetItems.Where(p => p.WorkingSetId == id);
            ViewBag.ID = id;
            ViewBag.reqs = db.req_Transfer_Columns_to_Records.Where(p=>p.CreatedBy== System.Web.HttpContext.Current.User.Identity.Name && p.WorkingSetId==id).OrderByDescending(p=>p.CreatedDate).ToList();
            return View(files);
        }
        public void IncludeTransferColumnsToRecord(int reqid)
        {
            var db = new BL.DA_Model();
            var req = db.req_Transfer_Columns_to_Records.FirstOrDefault(p => p.Id == reqid);
            
            if (req != null)
            {
                var ws = db.workingSets.FirstOrDefault(p => p.Id == req.WorkingSetId);
                //var firstFileWsItem = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == ws.Id);
                var firstFile = db.files.FirstOrDefault(p => p.State == ws.State &&
                  p.County == ws.County &&
                  p.User == System.Web.HttpContext.Current.User.Identity.Name 
                );
                //var path = "D:\\FA_in_out\\Tmp_data\\" + ws.State + "\\" + ws.County + "\\" + req.OutputName;
                var pathInput = "D:\\FA_in_out\\InputFile\\" + ws.State + "\\" + ws.County + "\\" + req.OutputName;
                if (!System.IO.File.Exists(pathInput))
                    throw new Exception("File not found");
                //create new file in db
                var foundfile = db.files.FirstOrDefault(p => p.County == ws.County && p.State == ws.State && p.Packageid == firstFile.Packageid && p.Name == req.OutputName);
                if (foundfile == null)
                {
                    var file = new BL.file();
                    file.County = ws.County;
                    file.Create_date = DateTime.Now;
                    file.Name = req.OutputName;
                    file.Packageid = firstFile.Packageid;
                    file.State = ws.State;
                    file.Status = "Processing";
                    file.User = System.Web.HttpContext.Current.User.Identity.Name;
                    db.files.Add(file);
                }


                var foundWsi = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == ws.Id && p.Filename == req.OutputName);
                if (foundWsi == null)
                {
                    var wsItem = new BL.WorkingSetItem();
                    //file.County = json.County;
                    //file.Create_date = date_create;
                    wsItem.Filename = req.OutputName;
                    //wsItem.PrimaryKey = item.PrimaryKey.ReplaceUnusedCharacters();
                    //wsItem.SecondaryKeys = item.SecondaryKeys;
                    wsItem.WorkingSetId = ws.Id;
                    //file. = "test";
                    db.workingSetItems.Add(wsItem);
                }
                db.SaveChanges();
            }
        }
        public ActionResult TaxInstallment(int id)
        {
            var db = new BL.DA_Model();
            var files = db.workingSetItems.Where(p => p.WorkingSetId == id);
            ViewBag.ID = id;
            ViewBag.reqs = db.req_Transfer_NumOfTaxInstallments.ToList();
            return View(files);
        }
        [HttpPost]
        public ActionResult postTaxInstallment(int wsiId, string[] columns, string newField, string newFile, int numOfTaxInstallment)
        {
            newFile = newFile.ReplaceUnusedCharacters();
            var strColumns = string.Join(";];", columns);
            newField = newField.ReplaceUnusedCharacters();

            var db_ = new BL.DA_Model();

            var wsi = db_.workingSetItems.Find(wsiId);
            if (wsi == null)
            {
                throw new Exception("WorkingSetItem not found!!");
            }

            var findReq = db.req_Transfer_NumOfTaxInstallments.FirstOrDefault(p => p.WorkingSetId == wsi.WorkingSetId && p.OutputName == newFile);
            if (findReq == null)
            {
                var r = new BL.Req_Transfer_NumOfTaxInstallment
                {
                    CreatedBy = System.Web.HttpContext.Current.User.Identity.Name,
                    CreatedDate = DateTime.Now,
                    IsDeleted = false,
                    IsReady = true,
                    New_Field_Name = newField,
                    OutputName = newFile,
                    Status = 0,
                    StrColumns = strColumns,
                    WorkingSetId = wsi.WorkingSetId,
                    WorkingSetItemId = wsiId,
                    NumOfTaxInstallment=numOfTaxInstallment
                };
                db.req_Transfer_NumOfTaxInstallments.Add(r);
                db.SaveChanges();
            }
            else
            {
                throw new Exception("This request is already existed");
            }
            
            return null;
        }
        [HttpPost]
        public ActionResult postTransferColumnsToRecord(int wsiId, string[] columns,string newField, string newFile,string strIgnoreNull, string strIgnoreZero)
        {
            newFile = newFile.ReplaceUnusedCharacters();
            var strColumns = string.Join(";];", columns);
            newField = newField.ReplaceUnusedCharacters();

            var db_ = new BL.DA_Model();

            var wsi = db_.workingSetItems.Find(wsiId);
            if (wsi == null)
            {
                throw new Exception("WorkingSetItem not found!!");
            }

            var findReq = db.req_Transfer_Columns_to_Records.FirstOrDefault(p => p.WorkingSetId == wsi.WorkingSetId && p.OutputName == newFile);
            if (findReq == null)
            {
                var r = new BL.Req_Transfer_Columns_to_Records
                {
                    CreatedBy = System.Web.HttpContext.Current.User.Identity.Name,
                    CreatedDate = DateTime.Now,
                    IsDeleted = false,
                    IsReady = true,
                    New_Field_Name = newField,
                    OutputName = newFile,
                    Status = 0,
                    StrColumns = strColumns,
                    WorkingSetId = wsi.WorkingSetId,
                    WorkingSetItemId = wsiId,
                    IgnoreOnNull=strIgnoreNull,
                    IgnoreOnZero=strIgnoreZero
                };
                db.req_Transfer_Columns_to_Records.Add(r);
                db.SaveChanges();
            }else
            {
                throw new Exception("This request is already existed");
            }
            //using (var client = new System.Net.WebClient())
            //{
            //    newFile = newFile.ReplaceUnusedCharacters();
            //    client.QueryString.Add("wsiId", wsiId.ToString());
            //    client.QueryString.Add("strcolumns", string.Join(";];", columns));// "col1;];col2;];col3;];");//params: ngan cach boi dau ;];
            //    client.QueryString.Add("toNewName", newField.ReplaceUnusedCharacters());
            //    client.QueryString.Add("newFileName", newFile);

            //    //var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/TransferColumnsToRecord");
            //    //using (var dbContextTransaction = db_.Database.BeginTransaction())
            //    //{
            //    //    var newFileId = 0;
            //    //    try
            //    //    {
            //    //        var wsi = db_.workingSetItems.Find(wsiId);
            //    //        if(!db.workingSetItems.Any(p=>p.WorkingSetId==wsi.WorkingSetId && p.Filename == newFile))
            //    //        {
            //    //            var db_newWsItem = new BL.WorkingSetItem();
            //    //            db_newWsItem.Filename = newFile;
            //    //            db_newWsItem.IsLayouted = false;
            //    //            db_newWsItem.IsMerged = false;
            //    //            db_newWsItem.PrimaryKey = wsi.PrimaryKey;
            //    //            db_newWsItem.WorkingSetId = wsi.WorkingSetId;
            //    //            db_.workingSetItems.Add(db_newWsItem);
            //    //            db_.SaveChanges();
            //    //            newFileId = db_newWsItem.Id;

            //        //            //db_.SaveChanges();
            //        //            dbContextTransaction.Commit();
            //        //        }



            //        //    }
            //        //    catch (Exception exT)
            //        //    {
            //        //        dbContextTransaction.Rollback();
            //        //        ModelState.AddModelError("", exT.InnerException.Message);
            //        //        GC.Collect();
            //        //        throw exT;
            //        //    }
            //        //    if(newFileId>0)
            //        //        GetLayout(newFileId);

            //        //}
            //        //var json = client.DownloadString(Url.Action("Index", "JobLayout", null, this.Request.Url.Scheme) + "/?id=" + fileid);

            //}
            return null;
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
                    var found = db.workingSets.FirstOrDefault(p => p.County == wd.ws.County &&
                      p.State == wd.ws.State &&
                      p.Version == wd.ws.Version &&
                      p.Edition == wd.ws.Edition);
                    var date_create = DateTime.Now;
                    var wsid = 0;
                    if (found == null)
                    {
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
                        wsid = workingSet.Id;
                    }else
                    {
                        wsid = found.Id;
                    }
                    
                    
                    //throw new Exception("fksljd");
                    //tạo các files thuộc về package
                    foreach (var item in wd.items)
                    {
                        var foundWsi = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == wsid && p.Filename == item.Filename);
                        if (foundWsi != null)
                            throw new Exception("File with name " + item.Filename + " is exited in the WorkingSet");
                        var wsItem = new BL.WorkingSetItem();
                        //file.County = json.County;
                        //file.Create_date = date_create;
                        wsItem.Filename = item.Filename;
                        wsItem.PrimaryKey = item.PrimaryKey.ReplaceUnusedCharacters();
                        wsItem.SecondaryKeys = item.SecondaryKeys;
                        wsItem.WorkingSetId = wsid;
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
                    ModelState.AddModelError("", exT.Message+"\n"+exT.StackTrace);
                    GC.Collect();
                    throw exT;
                }


            }
        }
        class mergeJSData
        {
            public BL.MergeFileJob orginal { get; set; }
            public List<BL.MergeDetail> mergeDetails { get; set; }
        }
        [HttpGet]
        public JsonResult getMergeList(int wsid)
        {
            var db = new BL.DA_Model();
            var ls = db.mergeFileJob.Where(p => p.WorkingJobId == wsid).ToList().Select(p => new mergeJSData()
            {
                orginal = p,
                mergeDetails = p.MergeDetails.XMLStringToListObject<BL.MergeDetail>()
            });
            return Json(ls, JsonRequestBehavior.AllowGet);
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