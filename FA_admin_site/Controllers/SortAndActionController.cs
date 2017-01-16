using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Libs;
using BL;
namespace FA_admin_site.Controllers
{
    [Authorize]
    public class SortAndActionController : Controller
    {
        
        public string TestPost(List<SortField> ls)
        {

            return "fjsdk";
        }
        // GET: SortAndAction
        /// <summary>
        /// workingSetItem(File)id
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public ActionResult Index(int id)
        {
            ViewBag.ID = id;
            var db = new BL.DA_Model();
            var wsFile = db.workingSetItems.Find(id);
            var ws = db.workingSets.FirstOrDefault(p => p.Id == wsFile.WorkingSetId);


            ViewBag.WorkingSetInfo = ws;
            ViewBag.Filename = wsFile.Filename;

            var rules = db.fieldRules.Where(p => p.WorkingSetItemId == id);
            ViewBag.Rules= Newtonsoft.Json.JsonConvert.SerializeObject(rules);
            ///
            var fields_sortAndAction = db.fieldOrderAndActions.FirstOrDefault(p => p.WorkingSetItemId == id);
            if (fields_sortAndAction == null)
            {
                using (var client = new System.Net.WebClient())
                {
                    //var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetFileInfo?state=" + job.State + "&county=" + job.County + "&filename=" + job.Filename);
                    var json = client.DownloadString(Config.Get_local_control_site() + "/JSON/GetHeader?state=" + ws.State + "&county=" + ws.County + "&filename=" + wsFile.Filename);
                    var headers = new System.Web.Script.Serialization.JavaScriptSerializer().Deserialize<string[]>(json);
                    var order = 1;
                    foreach (var header in headers)
                    {
                        var column_ = new BL.FieldOrderAndAction
                        {
                            DuplicatedAction = (int)DuplicateAction.PickupFirstUn_NULL_value,
                            DuplicatedActionType = 1,
                            FieldName = header.ReplaceUnusedCharacters(),
                            OrderType = (int)SortType.None,
                            WorkingSetItemId = wsFile.Id,
                            Order=order
                        };
                        db.fieldOrderAndActions.Add(column_);
                        
                        order++;
                    }
                    db.SaveChanges();
                }
            }
            var fields = db.fieldOrderAndActions.Where(p => p.WorkingSetItemId == id);
            //var fields =  db.fieldOrderAndActions.Where(p => wsFile.Any(c=>c.Id==p.WorkingSetItemId));
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
            ViewBag.Fields = Newtonsoft.Json.JsonConvert.SerializeObject(fields.OrderBy(p=>p.Order).Select(p=>p.FieldName));
            ViewBag.FieldTypes = Newtonsoft.Json.JsonConvert.SerializeObject(db.jobFileLayouts.Where(p=>p.WorkingSetItemId==id).Select(p=>new { name=p.Fieldname,type=p.Type}));
            ViewBag.Fileid = id;
            return View(fields);
        }
        [HttpPost]
        public void deleteRule(int id)
        {
            var db = new BL.DA_Model();
            var rule = db.fieldRules.Find(id);
            db.fieldRules.Remove(rule);
            db.SaveChanges();
        }
        [HttpPost]
        
        public string getRules(int id)
        {
            var db = new BL.DA_Model();
            var rules = db.fieldRules.Where(p => p.WorkingSetItemId == id);
            return Newtonsoft.Json.JsonConvert.SerializeObject(rules);
        }
        //[AllowHtml]
        [ValidateInput(false)]
        [HttpPost]
        public JsonResult addRuleMath(int fileid,BL.FieldRule rule)
        {
            //rule.WorkingSetItemId
            var db = new BL.DA_Model();
            var rules = db.fieldRules.Where(p => p.WorkingSetItemId == fileid);
            var count = rules.Count();
            var nameid = count > 0?rules.Max(p => p.NameID):0;
            var order= count > 0 ? rules.Max(p => p.Order):0;
            var name = "RULE_" + (nameid + 1);
            rule.WorkingSetItemId = fileid;
            rule.Name = name;
            rule.NameID = nameid + 1;
            rule.Order = order + 1;
            db.fieldRules.Add(rule);
            db.SaveChanges();
            return Json(rule, JsonRequestBehavior.AllowGet);
        }
        public void UpdateSortType(int id, int sort)
        {
            var db = new BL.DA_Model();
            var rec = db.fieldOrderAndActions.Find(id);
            rec.OrderType = sort;
            db.SaveChanges();
            db.Dispose();

        }
        public void UpdateAction(int id, int type, int action)
        {
            var db = new BL.DA_Model();
            var rec = db.fieldOrderAndActions.Find(id);
            rec.DuplicatedAction = action;
            rec.DuplicatedActionType = type;
            db.SaveChanges();
            db.Dispose();

        }
        public void UpdateAction_concat(int id, int type, int action,string delimeter)
        {
            var db = new BL.DA_Model();
            var rec = db.fieldOrderAndActions.Find(id);
            rec.DuplicatedAction = action;
            rec.DuplicatedActionType = type;
            rec.ConcatenateWithDelimiter = delimeter;
            db.SaveChanges();
            db.Dispose();

        }
    }
}
public enum DuplicateAction
{
    ResponseWithError = 0,
    PickupFirstValue = 1,
    PickupLastValue = 2,
    PickupFirstUn_NULL_value = 3,
    PickupMaximumValue = 4,
    PickupMinimumValue = 5,

    SumAllRow = 6,
    ConcatenateWithDelimiter = 7,


    KeepAllRows = 8,
    DropAllRows = 9,


}
public enum SortType
{
    None = 0,
    Asccending = 1,
    Deccending = 2
}
public class SortField
{
    public string name { get; set; }
    public DuplicateAction duplicateAction { get; set; }
    public SortType sortType { get; set; }
    /// <summary>
    /// 1: is Simple type as: Pickup first,last
    /// 2: Need one parameter like dilimeter
    /// 3: complex type
    /// </summary>
    public int duplicateActionType { get; set; }
    /// <summary>
    /// Phụ thuộc vào Type
    /// Nếu type=2, str_param là dilimeter
    /// </summary>
    public string str_param { get; set; }
}