using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FA_admin_site.Controllers
{
    public class OutputController : Controller
    {
        public class FileInfo
        {
            
            public int id { get; set; }
            public string name { get; set; }
            public string[] field { get; set; }
        }
        public class Field
        {
            public int OutId { get; set; }
            public int wsId { get; set; }
            public int fieldid { get; set; }
            public string targetField { get; set; }
            public string filemappername { get; set; }
            public string fieldmappername { get; set; }
        }
        // GET: Output
        public ActionResult Index()
        {
            var db = new BL.DA_Model();
            var outputFields = db.outputFields.OrderBy(p=>p.Order);
            ViewBag.OutputFields = JsonConvert.SerializeObject(outputFields);
            return View(db.outputMappers);
        }
        public ActionResult Select(int id)
        {
            ViewBag.Id = id;
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var all_outputFields = db.outputFields.OrderBy(p => p.Order);
            ViewBag.OutputFields = JsonConvert.SerializeObject(all_outputFields);

            var ws_files = db.workingSetItems.Where(p => p.WorkingSetId == id);
            //ViewBag.Files = ws_files;

            //var files = db.workingSetItems.Where(p => p.WorkingSetId == id);
            var rs_file = new List<FileInfo>();
            foreach (var file in ws_files)
            {
                var fileinfo = new FileInfo();
                fileinfo.name = file.Filename;
                fileinfo.id = file.Id;
                var columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == file.Id);
                fileinfo.field = columns.Select(p => p.Fieldname).ToArray();
                rs_file.Add(fileinfo);
            }
            //String serializedResult = /*JsonConvert.SerializeObject(jsonContactsGroups);*/
            ViewBag.Files = JsonConvert.SerializeObject(rs_file);
            //get data
            var outputFields = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            var data = from p in outputFields
                       join pp in db.outputDatas
                       on p.Id equals pp.OutputFieldId
                       select new {
                           target=p.Name,
                           Id=pp.Id,
                           OutputFieldId = pp.OutputFieldId,
                           FileMapperName = pp.FileMapperName,
                           FieldMapperName = pp.FieldMapperName
                       };
            ViewBag.UserOutputField= JsonConvert.SerializeObject(data);
            ViewBag.SeletedOutputId = ws.SeletedOutputId;
            return View(db.outputMappers);
        }
        [HttpPost]
        public string getUserData(int id)
        {
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var outputFields = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            var data = from p in outputFields
                       join pp in db.outputDatas
                       on p.Id equals pp.OutputFieldId
                       where pp.WorkingSetId==id
                       select new
                       {
                           target = p.Name,
                           Id = pp.Id,
                           OutputFieldId = pp.OutputFieldId,
                           FileMapperName = pp.FileMapperName,
                           FieldMapperName = pp.FieldMapperName
                       };
            return JsonConvert.SerializeObject(data);
        }
        [HttpPost]
        public void addData(Field data)
        {
            var db = new BL.DA_Model();
            //get field is existed
            var selectOutput = db.workingSets.FirstOrDefault(p=>p.Id==data.wsId);
            if (selectOutput.SeletedOutputId <= 0)
            {
                selectOutput.SeletedOutputId = data.OutId;
                db.SaveChanges();
            }else
            {
                if(selectOutput.SeletedOutputId != data.OutId)
                {
                    var outputFields = db.outputFields.Where(p => p.OutputMapperId == selectOutput.SeletedOutputId);
                    var allField= from p in outputFields
                                  join pp in db.outputDatas
                                  on p.Id equals pp.OutputFieldId
                                  select pp;
                    db.outputDatas.RemoveRange(allField);
                    selectOutput.SeletedOutputId = data.OutId;
                    db.SaveChanges();
                }
            }
            var field = db.outputDatas.FirstOrDefault(p => p.Id == data.fieldid && p.FieldMapperName==data.filemappername && p.FieldMapperName==data.fieldmappername);
            //var a = new BL.OutputData();
            //a.Data = " The code you provide at least looks better than my code because the validation constraint is declared in the attribute. How would the ProcessValidation see the MaxStringLength attribute and know what property it is working with? – Ben McCormack Sep 2 '10 at 14:00Reflectively.The ProcessValidation() method can know the type of your object(either this.GetType() or a similar call on a passed parameter), and from there it can get information for the member CompanyName and by extension the attributes decorating it.The attribute can be just a flag telling a central validator routine the exact rules to apply, or you can put the validation rule in the attribute and reflectively call some Evaluate() method on the attribute itself.Declarative validation can get messy, but that mess can be hidden behind the scenes unlike simple Validate() class members. – KeithS Sep 2 '10 at 15:28 ";

            if (field == null)
            {
                //not existed in db
                var new_field = new BL.OutputData();
                new_field.OutputFieldId = data.fieldid;
                new_field.FieldMapperName = data.fieldmappername;
                new_field.FileMapperName = data.filemappername;
                new_field.WorkingSetId = data.wsId;
                db.outputDatas.Add(new_field);
                db.SaveChanges();
            }
            //else
            //{

            //}

        }
        public ActionResult Map(int id)
        {
            var db = new BL.DA_Model();
            var outputFields = db.outputFields.OrderBy(p => p.Order);
            ViewBag.OutputFields = JsonConvert.SerializeObject(outputFields);
            return View(db.outputMappers);
        }
        [HttpPost]
        public void CreateNewOutput(BL.OutputMapper file)
        {
            var db = new BL.DA_Model();
            db.outputMappers.Add(file);
            db.SaveChanges();
        }
        [HttpPost]
        public void CreateNewField(BL.OutputFields field)
        {
            var db = new BL.DA_Model();
            var newOrder = db.outputFields.Where(p => p.OutputMapperId == field.OutputMapperId).Max(p => p.Order);
            field.Order = newOrder + 1;
            db.outputFields.Add(field);
            db.SaveChanges();
        }
        [HttpPost]
        public void del(int id)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id==id);
            db.outputFields.Remove(field);
            db.SaveChanges();
        }
        [HttpPost]
        public void re_order(List<BL.OutputFields> fields)
        {
            var new_order = 0;
            var db = new BL.DA_Model();
            var OutputMapperId = fields.First().OutputMapperId;
            var dbfields = db.outputFields.Where(p => p.OutputMapperId == OutputMapperId);
            foreach (var f in fields)
            {
                new_order++;
                var dbfield = dbfields.FirstOrDefault(p => p.Id == f.Id);
                dbfield.Order = new_order;
            }
            db.SaveChanges();
        }
        public void UpdateFieldName(int pk,string value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Name = value;
            db.SaveChanges();
        }
        public void UpdateFieldType(int pk, string value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Type = value;
            db.SaveChanges();
        }
        public void UpdateFieldLength(int pk, int value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Length = value;
            db.SaveChanges();
        }
        public void UpdateFieldDecimal(int pk, int value)
        {
            var db = new BL.DA_Model();
            var field = db.outputFields.FirstOrDefault(p => p.Id == pk);
            field.Decimal = value;
            db.SaveChanges();
        }
        
    }
}