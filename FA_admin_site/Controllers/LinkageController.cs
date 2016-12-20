using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using BL;
using Libs;
namespace FA_admin_site.Controllers
{
    public class LinkageController : Controller
    {
        public class FileInfo
        {
            public int id { get; set; }
            public string name { get; set; }
            public string[] field { get; set; }
        }
        // GET: Linkage
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id">workingSetId</param>
        /// <returns></returns>
        public ActionResult Index(int id)
        {
            var db = new BL.DA_Model();
            var files = db.workingSetItems.Where(p => p.WorkingSetId == id);
            var rs_file = new List<FileInfo>();
            foreach (var file in files)
            {
                var fileinfo = new FileInfo();
                fileinfo.name = file.Filename;
                fileinfo.id = file.Id;
                var columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == file.Id);
                fileinfo.field = columns.Select(p => p.Fieldname).ToArray();
                rs_file.Add(fileinfo);
            }
            String serializedResult = /*JsonConvert.SerializeObject(jsonContactsGroups);*/
            ViewBag.Files = JsonConvert.SerializeObject(rs_file);
            var xmlLinkageData = db.workingSets.FirstOrDefault(p => p.Id == id).Linkage;
            var linkageData = new List<LinkageItem>();
            if (!string.IsNullOrEmpty(xmlLinkageData))
                linkageData = xmlLinkageData.XMLStringToListObject<LinkageItem>();
            ViewBag.LinkageData = JsonConvert.SerializeObject(linkageData); ;
            ViewBag.Id = id;
            return View("LinkageIndex");
        }
        [HttpPost]
        public void createLinkageItem(LinkageItem rec,int id)
        {
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            if (ws == null)
                throw new Exception("could not found data");
            try
            {
                var linkageItems = new List<LinkageItem>();
                if (!string.IsNullOrEmpty(ws.Linkage))
                    linkageItems = ws.Linkage.XMLStringToListObject<LinkageItem>();


                if (linkageItems.Any(p=>p.firstId==rec.firstId && p.firstField==rec.firstField 
                    && p.sndId==rec.sndId && p.sndField==rec.sndField))
                    throw new Exception("duplicated data");
                linkageItems.Add(rec);
                ws.Linkage = linkageItems.XmlSerialize<LinkageItem>();
                db.SaveChanges();
            }
            catch (Exception ex)
            {

                throw new Exception("error");
            }

        }
        public void delLinkageItem(int id)
        {

        }
    }
}