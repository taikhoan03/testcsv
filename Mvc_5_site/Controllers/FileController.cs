using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Mvc_5_site.Controllers
{
    public class FileController : Controller
    {
        
        public FileResult Download(string state, string county,string filename)
        {
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            path = path + @"\" + filename;
            return Download_abs(path);
            //byte[] fileBytes = System.IO.File.ReadAllBytes(path);
            //string fileName = filename;// "myfile.ext";
            //return File(fileBytes, System.Net.Mime.MediaTypeNames.Application.Octet, fileName);
        }
        public FileResult Download_tmp(string state, string county, string filename)
        {
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("tmp_folder_process"),
                                    state,
                                    county
                                    );
            path = path + @"\" + filename;
            return Download_abs(path);
            //byte[] fileBytes = System.IO.File.ReadAllBytes(path);
            //string fileName = filename;// "myfile.ext";
            //return File(fileBytes, System.Net.Mime.MediaTypeNames.Application.Octet, fileName);
        }
        private FileResult Download_abs(string path)
        {
            
            byte[] fileBytes = System.IO.File.ReadAllBytes(path);
            string fileName = Path.GetFileName(path);// "myfile.ext";
            return File(fileBytes, System.Net.Mime.MediaTypeNames.Application.Octet, fileName);
        }

        // GET: File
        //public ActionResult Index(string state, string county)
        //{
        //    if (!string.IsNullOrEmpty(state) && !string.IsNullOrEmpty(county))
        //    {
        //        string path = Path.Combine(Config.Data.GetKey("root_folder_process"),
        //                            Config.Data.GetKey("input_folder_process"),
        //                            state,
        //                            county
        //                            );
        //        var files = Directory.GetFiles(path, "*.*").Select(p => Path.GetFileName(p));
        //        var dA_Model = (new BL.DA_Model());
        //        var db_files = dA_Model.files.Where(p => p.State == state && p.County == county);
        //        var rs = from p in files
        //             from pp in db_files.Where(x=>p==x.Name).DefaultIfEmpty()
        //             select new BL.file
        //             {
        //                 Name = p,
        //                 User = pp == null ? null : pp.User,
        //                 County = pp == null ? null : pp.County,
        //                 Create_date = pp == null ? new DateTime(2000,1,1) : pp.Create_date,
        //                 Id= pp == null ? 0 : pp.Id,
        //                 Is_deleted = pp == null ? false : pp.Is_deleted,
        //                 State = pp == null ? null : pp.State,
        //             };
        //        return View(rs.OrderByDescending(p=>p.Create_date));
        //    }
        //    else if (!string.IsNullOrEmpty(state))
        //    {
        //        //return View((new BL.DA_Model()).files.Where(p => p.State == state));

        //        string path = Path.Combine(Config.Data.GetKey("root_folder_process"),
        //                            Config.Data.GetKey("input_folder_process"),
        //                            state
        //                            );
        //        var files = DirSearch(path).Select(p => Path.GetFileName(p));// Directory.GetFiles(path, "*.*").Select(p => Path.GetFileName(p));
        //        var dA_Model = (new BL.DA_Model());
        //        var db_files = dA_Model.files.Where(p => p.State == state);
        //        var rs = from p in files
        //                 from pp in db_files.Where(x => p == x.Name).DefaultIfEmpty()
        //                 select new BL.file
        //                 {
        //                     Name = p,
        //                     User = pp == null ? null : pp.User,
        //                     County = pp == null ? null : pp.County,
        //                     Create_date = pp == null ? new DateTime(2000, 1, 1) : pp.Create_date,
        //                     Id = pp == null ? 0 : pp.Id,
        //                     Is_deleted = pp == null ? false : pp.Is_deleted,
        //                     State = pp == null ? null : pp.State,
        //                 };
        //        return View(rs.OrderByDescending(p => p.Create_date));
        //    }
        //    //else if (!string.IsNullOrEmpty(county))
        //    //{
        //    //    //return View((new BL.DA_Model()).files.Where(p => p.County == county));
        //    //    string path = Path.Combine(Config.Data.GetKey("root_folder_process"),
        //    //                        Config.Data.GetKey("input_folder_process"),
        //    //                        state
        //    //                        );
        //    //    var files = DirSearch(path).Select(p => Path.GetFileName(p));// Directory.GetFiles(path, "*.*").Select(p => Path.GetFileName(p));
        //    //    var dA_Model = (new BL.DA_Model());
        //    //    var db_files = dA_Model.files.Where(p => p.County == county);
        //    //    var rs = from p in files
        //    //             from pp in db_files.Where(x => p == x.Name).DefaultIfEmpty()
        //    //             select new BL.file
        //    //             {
        //    //                 Name = p,
        //    //                 User = pp == null ? null : pp.User,
        //    //                 County = pp == null ? null : pp.County,
        //    //                 Create_date = pp == null ? new DateTime(2000, 1, 1) : pp.Create_date,
        //    //                 Id = pp == null ? 0 : pp.Id,
        //    //                 Is_deleted = pp == null ? false : pp.Is_deleted,
        //    //                 State = pp == null ? null : pp.State,
        //    //             };
        //    //    return View(rs);
        //    //}
        //    return View((new BL.Class1()).getFiles());
        //}
        //public ActionResult Create()
        //{
        //    return View();
        //}

        //// POST: Blank/Create
        //[HttpPost]
        //[ValidateAntiForgeryToken]
        //public ActionResult Create([Bind(Include = "User,State,County,Name")] BL.file file)
        //{
        //    try
        //    {
        //        // TODO: Add insert logic here
        //        if (ModelState.IsValid)
        //        {
        //            file.User = "test";
        //            file.Create_date = DateTime.Now;
        //            var db = new BL.DA_Model();
        //            db.files.Add(file);
        //            db.SaveChanges();
        //            return RedirectToAction("Index");
        //            //db.Entry(movie).State = EntityState.Modified;
        //            //db.SaveChanges();
        //        }
        //        ModelState.AddModelError("", "Unable to perform action. Please contact us.");
        //        //return View(file);
        //    }
        //    catch (Exception ex)
        //    {
        //        ModelState.AddModelError("", "Unable to perform action. Please contact us.");
        //        //return RedirectToAction("SubmissionFailed", collection);

        //    }
        //    return View(file);
        //}
        //private List<String> DirSearch(string sDir)
        //{
        //    List<String> files = new List<String>();
        //    try
        //    {
        //        foreach (string f in Directory.GetFiles(sDir))
        //        {
        //            files.Add(f);
        //        }
        //        foreach (string d in Directory.GetDirectories(sDir))
        //        {
        //            files.AddRange(DirSearch(d));
        //        }
        //    }
        //    catch (System.Exception excpt)
        //    {
        //        //MessageBox.Show(excpt.Message);
        //    }

        //    return files;
        //}
    }
}