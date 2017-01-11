using BL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace FA_admin_site.Controllers
{
    public class OutputRuleMapperController : Controller
    {
        // GET: OutputRuleMapper
        public ActionResult Index(int wsid)
        {
            ViewBag.ID = wsid;
            
            var db = new BL.DA_Model();
            var wsFiles = db.workingSetItems.Where(p=>p.WorkingSetId==wsid).ToList();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == wsid);
            ViewBag.OutputFileId = ws.SeletedOutputId;

            ViewBag.WorkingSetInfo = ws;
            ViewBag.Filename = "test";

            var rules = db.fieldRules.Where(p => p.WorkingSetItemId == wsid);
            ViewBag.Rules = Newtonsoft.Json.JsonConvert.SerializeObject(rules);
            ///
            
            var fields = db.fieldOrderAndActions.Where(p => p.WorkingSetItemId == wsid);
            //List<BL.JobFileLayout> rs = new List<BL.JobFileLayout>();
            //foreach (var field in fields)
            //{
            //    var column = new BL.JobFileLayout
            //    {
            //        WorkingSetItemId = id,
            //        Fieldname = field.FieldName,
            //        Id=field.Id,

            //        //Mapper = "{" + field.FieldName.Replace(" ", "_") + "}",
            //        Order = field.Order
            //    };

            //    rs.Add(column);
            //}
            ViewBag.Fields = Newtonsoft.Json.JsonConvert.SerializeObject(fields.OrderBy(p => p.Order).Select(p => p.FieldName));
            var fieldsDB = from p in wsFiles
                           join pp in db.jobFileLayouts
                           on p.Id equals pp.WorkingSetItemId
                           select new
                           {
                               id=pp.Id,
                               name = p.Filename + ":" + pp.Fieldname,
                               type = pp.Type
                           };
            //ViewBag.FieldTypes = Newtonsoft.Json.JsonConvert.SerializeObject(db.jobFileLayouts.Where(p => p.WorkingSetItemId == wsid).Select(p => new { name = .+p.Fieldname, type = p.Type }));
            ViewBag.FieldTypes = Newtonsoft.Json.JsonConvert.SerializeObject(fieldsDB);
            ViewBag.Fileid = wsid;
            //return View(fields);
            return View("OutputRuleMapperIndex",fields);
        }
        [HttpPost]
        public string getRuleFields(int fieldId)
        {
            var db = new BL.DA_Model();
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == fieldId);
            return Newtonsoft.Json.JsonConvert.SerializeObject(rules);
        }
        [HttpPost]
        public JsonResult addRule(int OutputFileId, int fieldid, BL.OutputDataDetail rule)
        {
            //rule.WorkingSetItemId
            var db = new BL.DA_Model();
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == OutputFileId && p.OutputFieldId==fieldid);
            var count = rules.Count();
            var nameid = count > 0 ? rules.Max(p => p.NameID) : 0;
            var order = count > 0 ? rules.Max(p => p.Order) : 0;
            var name = "RULE_" + (nameid + 1);
            rule.OutputFileId = OutputFileId;
            rule.OutputFieldId = fieldid;


            //rule. = fileid;
            rule.Name = name;
            rule.NameID = nameid + 1;
            rule.Order = order + 1;

            db.outputDataDetails.Add(rule);
            db.SaveChanges();
            return Json(rule, JsonRequestBehavior.AllowGet);
        }
    }
}