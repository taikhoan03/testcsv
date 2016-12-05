using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using Libs;
namespace FA_admin_site.Controllers
{
    [Authorize]
    public class FileController : Controller
    {
        public string test()
        {
            var db = new BL.DA_Model();
            var a = db.packages.FirstOrDefault();
            return a.State;
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
                    var dA_Model = (new BL.DA_Model());
                    var db_files = dA_Model.files.Where(p => p.State == state && p.County == county);
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
            var db = new BL.DA_Model();
            using (var dbContextTransaction = db.Database.BeginTransaction())
            {
                try
                {
                    if (ModelState.IsValid)
                    {

                        var date_create = DateTime.Now;
                        //tạo package
                        var package = new BL.package();
                        package.County = json.County;
                        package.State = json.State;
                        package.Createdby = "test";
                        package.Createddate = date_create;
                        package.Edition = json.Edition;
                        package.Version = json.Version;
                        package.Status = "Processing";
                        db.packages.Add(package);
                        db.SaveChanges();
                        //throw new Exception("fksljd");
                        //tạo các files thuộc về package
                        foreach (var name in json.Filenames)
                        {
                            var file = new BL.file();
                            file.County = json.County;
                            file.Create_date = date_create;
                            file.Name = name;
                            file.Packageid = package.Id;
                            file.State = json.State;
                            file.Status = "Processing";
                            file.User = "test";
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