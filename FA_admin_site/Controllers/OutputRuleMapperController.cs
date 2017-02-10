using BL;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Libs;
namespace FA_admin_site.Controllers
{
    [Authorize]
    public class OutputRuleMapperController : Controller
    {
        // GET: OutputRuleMapper
        public ActionResult Index(int wsid)
        {
            ViewBag.ID = wsid;
            
            var db = new BL.DA_Model();
            var wsFiles = db.workingSetItems.Where(p=>p.WorkingSetId==wsid);
            var ws = db.workingSets.FirstOrDefault(p => p.Id == wsid);
            ViewBag.OutputFileId = ws.SeletedOutputId;

            ViewBag.WorkingSetInfo = ws;
            ViewBag.Filename = "test";

            var rules = db.fieldRules.Where(p => p.WorkingSetItemId == wsid);
            ViewBag.Rules = Newtonsoft.Json.JsonConvert.SerializeObject(rules);
            ///

            var fields_ = from p in db.fieldOrderAndActions.Where(p => wsFiles.Any(c => c.Id == p.WorkingSetItemId))
                         join pp in db.workingSetItems.Where(p=>p.WorkingSetId==wsid)
                         on p.WorkingSetItemId equals pp.Id
                         select new { FieldName =pp.Filename+":"+p.FieldName, Order=p.Order}
                         ;// 
            var fields=db.fieldOrderAndActions.Where(p => wsFiles.Any(c=>c.Id==p.WorkingSetItemId));
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
            ViewBag.Fields = Newtonsoft.Json.JsonConvert.SerializeObject(fields_.OrderBy(p => p.Order).Select(p => p.FieldName));
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
        public ActionResult RunTransform(int id)
        {
            ViewBag.ID = id;
            return View();
        }
        public ActionResult TransformLog()
        {
            var db = new BL.DA_Model();
            var rs = db.runTransformRequests.Take(200).ToList();
            return View(rs);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="wsid"></param>
        /// <returns></returns>
        public string addRunTransformRequest(int wsid)
        {
            var db = new BL.DA_Model();
            var req = new BL.RunTransformRequest();
            req.CreatedBy = System.Web.HttpContext.Current.User.Identity.Name;
            req.CreatedDate = DateTime.Now;
            req.IsDeleted = false;
            req.Status = 0;
            req.WorkingSetId = wsid;
            //check co du lieu de san sang chay
            //check has primarykey
            var wsFiles = db.workingSetItems.Where(p=>p.WorkingSetId==wsid).ToList();
            var hasPrimaryKey = false;
            foreach (var item in wsFiles)
            {
                if (!string.IsNullOrEmpty(item.PrimaryKey))
                {
                    hasPrimaryKey = true;
                    break;
                }
            }
            //check has RuleMapper
            var hasRule = false;
            var rule_count = db.outputDataDetails.Where(p => p.WorkingSetId == wsid).Count();
            if (rule_count > 0)
                hasRule = true;
            if (hasRule && hasPrimaryKey)
            {
                req.IsReady = true;
            }
            var item_found = db.runTransformRequests.FirstOrDefault(p => p.WorkingSetId == wsid);
            db.runTransformRequests.Add(req);
            db.SaveChanges();
            return "OK";
        }
        public string addRunTransformRequest_ReRun(int wsid)
        {
            var db = new BL.DA_Model();
            //var req = new BL.RunTransformRequest();
            //req.CreatedBy = System.Web.HttpContext.Current.User.Identity.Name;
            //req.CreatedDate = DateTime.Now;
            //req.IsDeleted = false;
            //req.Status = 0;
            //req.WorkingSetId = wsid;
            var item_found = db.runTransformRequests.FirstOrDefault(p => p.WorkingSetId == wsid);
            if (item_found.Status != 1)//not processing
            {
                item_found.Status = 0;
                db.SaveChanges();
                return "OK";// "This RunTransform request has been created already and will be rerun again soon";
            }
            else
            {
                return "This RunTransform request has been created already and running currently";
            }

        }
        public string detachRequest(int wsid)
        {
            var db = new BL.DA_Model();
            //var req = new BL.RunTransformRequest();
            //req.CreatedBy = System.Web.HttpContext.Current.User.Identity.Name;
            //req.CreatedDate = DateTime.Now;
            //req.IsDeleted = false;
            //req.Status = 0;
            //req.WorkingSetId = wsid;
            var item_found = db.runTransformRequests.FirstOrDefault(p => p.WorkingSetId == wsid);
            if (item_found.Status != 1)//not processing
            {
                item_found.Status = 4;
                item_found.IsReady = false;
                db.SaveChanges();
                return "OK";// "This RunTransform request has been created already and will be rerun again soon";
            }
            else
            {
                return "This RunTransform request has been created already and running currently";
            }

        }
        public string verifyBeforeAddRequest(int wsid)
        {
            var db = new BL.DA_Model();
            //check has primarykey
            var wsFiles = db.workingSetItems.Where(p => p.WorkingSetId == wsid).ToList();
            var hasPrimaryKey = false;
            foreach (var item in wsFiles)
            {
                if (!string.IsNullOrEmpty(item.PrimaryKey))
                {
                    hasPrimaryKey = true;
                    break;
                }
            }
            //check has RuleMapper
            var hasRule = false;
            var rule_count = db.outputDataDetails.Where(p => p.WorkingSetId == wsid).Count();
            if (rule_count > 0)
                hasRule = true;
            if(!hasRule || !hasPrimaryKey)
            {
                return "Your request should not ready!!! (Missing Primary or rule to transform)";
            }
            var item_found = db.runTransformRequests.FirstOrDefault(p => p.WorkingSetId == wsid);
            
            return Newtonsoft.Json.JsonConvert.SerializeObject(item_found);
        }
        [HttpPost]
        public string getRuleFields(int fieldId, int wsid)
        {
            var db = new BL.DA_Model();
            var rules = db.outputDataDetails.Where(p => p.OutputFieldId == fieldId && p.WorkingSetId== wsid).ToList();
            return Newtonsoft.Json.JsonConvert.SerializeObject(rules);
        }
        [HttpPost]
        [ValidateInput(false)]
        public JsonResult addRule(int wsid, int OutputFileId, int fieldid, BL.OutputDataDetail rule)
        {
            //rule.WorkingSetItemId
            var db = new BL.DA_Model();
            //var wsid = db.workingSetItems.FirstOrDefault(p => p.Id == OutputFileId).WorkingSetId;

            var rules = db.outputDataDetails.Where(p => p.OutputFileId == OutputFileId && p.OutputFieldId==fieldid);
            var count = rules.Count();
            var nameid = count > 0 ? rules.Max(p => p.NameID) : 0;
            var order = count > 0 ? rules.Max(p => p.Order) : 0;
            var name = "RULE_" + (nameid + 1);
            rule.OutputFileId = OutputFileId;
            rule.OutputFieldId = fieldid;
            rule.WorkingSetId = wsid;
            var placeholders = rule.ExpValue.FindPlaceHolder();
            foreach (var item in placeholders)
            {
                //var a = 1;
                if (item.StartsWith("RULE_"))
                {
                    var new_match = rule.OutputFieldId + EV.DOLLAR + item;
                    rule.ExpValue = rule.ExpValue.Replace("{" + item + "}", "{" + new_match + "}");
                }
                
            }
            //rule. = fileid;
            rule.Name = name;
            rule.NameID = nameid + 1;
            rule.Order = order + 1;

            db.outputDataDetails.Add(rule);
            db.SaveChanges();
            return Json(rule, JsonRequestBehavior.AllowGet);
        }
        [HttpPost]
        public void deleteRule(int id)
        {
            var db = new BL.DA_Model();
            var rule = db.outputDataDetails.Find(id);
            db.outputDataDetails.Remove(rule);
            db.SaveChanges();
        }
    }
}