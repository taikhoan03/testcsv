using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;

using Libs;
using System.Threading.Tasks;
using System.Text;

namespace FA_admin_site.Controllers
{
    [Authorize]
    public class FileController : Controller
    {
        private BL.DA_Model db = new BL.DA_Model();
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                db.Dispose();
            }
            base.Dispose(disposing);
        }
        public ActionResult FixCSV1(int fileid)
        {
            var file = db.files.Find(fileid);
            var package = db.packages.FirstOrDefault(p => p.Id == file.Packageid);
            ViewBag.name = file.Name;
            ViewBag.state = package.State;
            ViewBag.county = package.County;
            ViewBag.fileid = fileid;
            return View();
        }
        public void RunFixCSV1(int fileid,string delimiter)
        {
            var file = db.files.Find(fileid);
            var package = db.packages.FirstOrDefault(p => p.Id == file.Packageid);
            var path = Path.Combine("D:\\FA_in_out",
                                        "InputFile",
                                        package.State,
                                        package.County,
                                        file.Name
                                        );
            if (delimiter == "t")
                delimiter = "\t";
            var sb = BL.Ulti.Fix_csv(path, delimiter);
            string text = sb.ToString();

            Response.Clear();
            Response.ClearHeaders();

            Response.AddHeader("Content-Length", text.Length.ToString());
            Response.ContentType = "text/plain";
            Response.AppendHeader("content-disposition", "attachment;filename=\"output.txt\"");

            Response.Write(text);
            Response.End();
            //          root_folder_process = D:\FA_in_out
            //input_folder_process = InputFile
            //path = path + @"\" + ;
        }
        public void test()
        {
            //var db = new BL.DA_Model();
            StringBuilder sb = new StringBuilder();
            string output = "Output";
            sb.Append(output);
            sb.Append("\r\n");

            string text = sb.ToString();

            Response.Clear();
            Response.ClearHeaders();

            Response.AddHeader("Content-Length", text.Length.ToString());
            Response.ContentType = "text/plain";
            Response.AppendHeader("content-disposition", "attachment;filename=\"output.txt\"");

            Response.Write(text);
            Response.End();
        }
        
        public ActionResult List(int packid)
        {
            //var db = new BL.DA_Model();
            var files = db.files.Where(p => p.Packageid == packid);
            return View(files);
        }

        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<ActionResult> DeleteConfirmed(int id)
        {
            //var db = new BL.DA_Model();
            
            var file = await db.files.FindAsync(id);
            //var package = db.packages.FirstOrDefault(p => p.Id == file.Packageid);
            var workingSets = db.workingSets.Where(p => p.County == file.County && p.State == file.State).ToList();
            var found = false;
            var wsid = 0;
            foreach (var item in workingSets)
            {
                if (found) break;
                var wsi = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == item.Id && p.Filename == file.Name);
                if (wsi != null)
                {
                    found = true;
                    wsid = wsi.WorkingSetId;
                    //db.workingSetItems.Remove(wsi);
                    //var path= Path.Combine(@"D:\FA_in_out\InputFile",
                    //                    item.State,
                    //                    item.County
                    //                    , wsi.Filename);
                    //System.IO.File.Delete(path);
                }
            }

            //var foundInWorkingSet = db.workingSetItems.Any(p => p.)
            if (found)
            {
                throw new Exception("This file has used in a WorkingSet");
                
            }
            db.files.Remove(file);
            await db.SaveChangesAsync();
            return RedirectToAction("List",new { packid=file.Packageid});
        }
        // GET: File
        public ActionResult Index(string state, string county,string term)
        {
            if (!string.IsNullOrEmpty(state) && !string.IsNullOrEmpty(county))
            {
                //string path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                //                    Config.Data.GetKey("input_folder_process"),
                //                    state,
                //                    county
                //                    );
                //var files = Directory.GetFiles(path, "*.*").Select(p => Path.GetFileName(p));
                //var files = new string[] { };
                using (var client = new WebClient())
                {
                    var json = client.DownloadString(Config.Get_local_control_site()+ "/JSON/GetFileWithStateAndCounty?state=" + state + "&county=" + county+"&term="+ term);
                    var files = new System.Web.Script.Serialization.JavaScriptSerializer().Deserialize<BL.file[]>(json);
                    //var db = (new BL.DA_Model());
                    var db_files = db.files.Where(p => p.State == state && p.County == county);
                    var rs = from p in files
                             from pp in db_files.Where(x => p.Name == x.Name).DefaultIfEmpty()
                             select new BL.file
                             {
                                 Name = p.Name,
                                 User = pp == null ? null : pp.User,
                                 County = county,
                                 Create_date = pp == null ? new DateTime(2000, 1, 1) : pp.Create_date,
                                 Id = pp == null ? 0 : pp.Id,
                                 Is_deleted = pp == null ? false : pp.Is_deleted,
                                 State = state,
                             };
                    return View(rs.OrderByDescending(p => p.Create_date));
                }
                
            }
            else if (!string.IsNullOrEmpty(state))
            {
                //return View((new BL.DA_Model()).files.Where(p => p.State == state));

                using (var client = new WebClient())
                {
                    var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetFileWithState?state=" + state+ "&term="+ term);
                    var files = new System.Web.Script.Serialization.JavaScriptSerializer().Deserialize<BL.file[]>(json);
                    //Path.GetFileName(p));
                    var dA_Model = (new BL.DA_Model());
                    var db_files = dA_Model.files.Where(p => p.State == state);
                    var rs = from p in files
                             from pp in db_files.Where(x => p.Name == x.Name).DefaultIfEmpty()
                             select new BL.file
                             {
                                 Name = p.Name,
                                 User = pp == null ? null : pp.User,
                                 County = p.County,
                                 Create_date = pp == null ? new DateTime(2000, 1, 1) : pp.Create_date,
                                 Id = pp == null ? 0 : pp.Id,
                                 Is_deleted = pp == null ? false : pp.Is_deleted,
                                 State = state,
                             };
                    return View(rs.OrderByDescending(p => p.Create_date));
                }
                
            }
            //else if (!string.IsNullOrEmpty(county))
            //{
            //    //return View((new BL.DA_Model()).files.Where(p => p.County == county));
            //    string path = Path.Combine(Config.Data.GetKey("root_folder_process"),
            //                        Config.Data.GetKey("input_folder_process"),
            //                        state
            //                        );
            //    var files = DirSearch(path).Select(p => Path.GetFileName(p));// Directory.GetFiles(path, "*.*").Select(p => Path.GetFileName(p));
            //    var dA_Model = (new BL.DA_Model());
            //    var db_files = dA_Model.files.Where(p => p.County == county);
            //    var rs = from p in files
            //             from pp in db_files.Where(x => p == x.Name).DefaultIfEmpty()
            //             select new BL.file
            //             {
            //                 Name = p,
            //                 User = pp == null ? null : pp.User,
            //                 County = pp == null ? null : pp.County,
            //                 Create_date = pp == null ? new DateTime(2000, 1, 1) : pp.Create_date,
            //                 Id = pp == null ? 0 : pp.Id,
            //                 Is_deleted = pp == null ? false : pp.Is_deleted,
            //                 State = pp == null ? null : pp.State,
            //             };
            //    return View(rs);
            //}
            return View((new BL.Class1()).getFiles());
        }
        public ActionResult Create(string state,string county,string filename)
        {
            ViewBag.State = state;
            ViewBag.County = county;
            ViewBag.Filename = filename;
            return View();
        }
        public class PackageModel
        {
            //public int ID { get; set; }
            public string State { get; set; }
            public string County { get; set; }
            public string[] Filenames { get; set; }
            public int Version { get; set; }
            public int Edition { get; set; }
        }
        // POST: Blank/Create
        [HttpPost]
        //[ValidateAntiForgeryToken]
        public void Create(PackageModel json)
        {
            //var db = new BL.DA_Model();
            using (var dbContextTransaction = db.Database.BeginTransaction())
            {
                try
                {
                    if (ModelState.IsValid)
                    {

                        var date_create = DateTime.Now;
                        //tạo package
                        var existedPackage = db.packages.FirstOrDefault(p => p.State == json.State && p.County == json.County 
                        && p.Edition == json.Edition && p.Version == json.Version
                        && p.Createdby== System.Web.HttpContext.Current.User.Identity.Name);
                        var package = new BL.package();
                        if (existedPackage != null)
                        {
                            package = existedPackage;
                        }else
                        {
                            package.County = json.County;
                            package.State = json.State;
                            package.Createdby = System.Web.HttpContext.Current.User.Identity.Name;
                            package.Createddate = date_create;
                            package.Edition = json.Edition;
                            package.Version = json.Version;
                            package.Status = "Processing";

                            db.packages.Add(package);
                            db.SaveChanges();
                        }

                        var filesInPackage = db.files.Where(p => p.Packageid == package.Id).ToList();
                        var filteredName = new List<string>();
                        filteredName = json.Filenames.Except(filesInPackage.Select(c=>c.Name).ToList()).ToList();
                        //throw new Exception("fksljd");
                        //tạo các files thuộc về package
                        foreach (var name in filteredName)//json.Filenames)
                        {
                            var file = new BL.file();
                            file.County = json.County;
                            file.Create_date = date_create;
                            file.Name = name;
                            file.Packageid = package.Id;
                            file.State = json.State;
                            file.Status = "Processing";
                            file.User = System.Web.HttpContext.Current.User.Identity.Name;
                            db.files.Add(file);
                        }


                        db.SaveChanges();
                        dbContextTransaction.Commit();
                        //return;// RedirectToAction("Index");
                        //db.Entry(movie).State = EntityState.Modified;
                        //db.SaveChanges();
                    }

                }
                catch (Exception exT)
                {
                    dbContextTransaction.Rollback();
                    ModelState.AddModelError("", exT.InnerException.Message);
                    throw exT;
                }


            }
            
        }
        
        private List<String> DirSearch(string sDir)
        {
            List<String> files = new List<String>();

            try
            {
                //foreach (string f in Directory.GetFiles(sDir))
                //{
                //    files.Add(f);
                //}
                foreach (string d in Directory.GetDirectories(sDir))
                {
                    files.AddRange(DirSearch(d));
                }
            }
            catch (System.Exception excpt)
            {
                //MessageBox.Show(excpt.Message);
            }

            return files;
        }
        
        [HttpPost]
        public void addHeader(string[] headers, string path)
        {
            using (var client = new System.Net.WebClient())
            {
                var reqparm = new System.Collections.Specialized.NameValueCollection();
                reqparm.Add("path", path);
                client.AddArray("headers", headers);
                byte[] responsebytes = client.UploadValues(Config.Get_local_control_site() + "/JSON/addHeader", "POST", reqparm);
                
                string responsebody = System.Text.Encoding.UTF8.GetString(responsebytes);
                if (!string.IsNullOrEmpty(responsebody))
                {
                    throw new Exception(responsebody);
                }

                
                //return Json("", JsonRequestBehavior.AllowGet);
            }
        }
    }
}