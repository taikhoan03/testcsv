using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using BL;
using Libs;
namespace FA_admin_site.Controllers
{
    public class TestRuleController : Controller
    {
        // GET: TestRule
        public ActionResult Index(int wsid)
        {
            ViewBag.ID = wsid;
            var db = new BL.DA_Model();
            var ws = db.workingSets.Find(wsid);
            // apply rule mapper
            var outputFields = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            var outputData = db.outputDatas.Where(p => outputFields.Any(c => c.Id == p.OutputFieldId) && p.WorkingSetId == ws.Id);

            var outputDataWithName = from p in outputData
                                     join pp in outputFields
                                     on p.OutputFieldId equals pp.Id
                                     select new
                                     {
                                         FieldMapperName = p.FieldMapperName,
                                         FileMapperName = p.FileMapperName,
                                         Id = p.Id,
                                         Order = p.Order,
                                         OutputFieldId = p.OutputFieldId,
                                         WorkingSetId = p.WorkingSetId,
                                         FieldName = pp.Name
                                     };
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId && p.WorkingSetId == wsid).ToList();//.OrderBy(p => p.Order);
            var seq1Name = "seq1";
            var seq2Name = "seq2";
            var outputDataWithNameList = outputDataWithName.ToList();
            outputDataWithNameList.Add(new
            {
                FieldMapperName = seq1Name,
                FileMapperName = seq1Name,
                Id = 0,
                Order = 999,
                OutputFieldId = -1,
                WorkingSetId = ws.Id,
                FieldName = seq1Name
            });
            outputDataWithNameList.Add(new
            {
                FieldMapperName = seq2Name,
                FileMapperName = seq2Name,
                Id = 0,
                Order = 999,
                OutputFieldId = -2,
                WorkingSetId = ws.Id,
                FieldName = seq2Name
            });
            var outputData_ = outputDataWithNameList.GroupBy(c => c.OutputFieldId);

            var rule_ = rules.ToList();
            var filteredRules = rule_.Where(p => !p.ExpValue.Contains("RULE_"));
            var concatAll = string.Join(" ", filteredRules.Select(p => p.ExpValue));
            //foreach (var rule in rule_)
            //{
            //    rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            //}


            var fields = concatAll.GetPlaceHolderName_ExpandObject();
            return View(fields);
        }
        public class pairNameValue
        {
            public string name { get; set; }
            public string val { get; set; }
        }
        [HttpPost]
        public string runTestRules(int wsid, List<pairNameValue> fields)
        {
            var db = new BL.DA_Model();
            var ws = db.workingSets.Find(wsid);
            // apply rule mapper
            var outputFields = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            var outputData = db.outputDatas.Where(p => outputFields.Any(c => c.Id == p.OutputFieldId) && p.WorkingSetId == ws.Id);

            var outputDataWithName = from p in outputData
                                     join pp in outputFields
                                     on p.OutputFieldId equals pp.Id
                                     select new
                                     {
                                         FieldMapperName = p.FieldMapperName,
                                         FileMapperName = p.FileMapperName,
                                         Id = p.Id,
                                         Order = p.Order,
                                         OutputFieldId = p.OutputFieldId,
                                         WorkingSetId = p.WorkingSetId,
                                         FieldName = pp.Name
                                     };
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId && p.WorkingSetId == wsid).ToList();//.OrderBy(p => p.Order);
            var seq1Name = "seq1";
            var seq2Name = "seq2";
            var outputDataWithNameList = outputDataWithName.ToList();
            outputDataWithNameList.Add(new
            {
                FieldMapperName = seq1Name,
                FileMapperName = seq1Name,
                Id = 0,
                Order = 999,
                OutputFieldId = -1,
                WorkingSetId = ws.Id,
                FieldName = seq1Name
            });
            outputDataWithNameList.Add(new
            {
                FieldMapperName = seq2Name,
                FileMapperName = seq2Name,
                Id = 0,
                Order = 999,
                OutputFieldId = -2,
                WorkingSetId = ws.Id,
                FieldName = seq2Name
            });
            var outputData_ = outputDataWithNameList.GroupBy(c => c.OutputFieldId);

            var rule_ = rules.ToList();
            foreach (var rule in rule_)
            {
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOLLAR).Replace(":", EV.DOLLAR); ;//rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }

            foreach (var field in fields)
            {
                field.name = field.name.Replace(".", EV.DOLLAR).Replace(":", EV.DOLLAR);//field.name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }


            var dyna = new DynaExp();
            var dt = new System.Data.DataTable();
            //create record for test
            var _ls = new List<IDictionary<string, object>>();
            var _rec = new Dictionary<string, object>();
            foreach (var item in fields)
            {
                _rec.Add(item.name, item.val);
            }
            _ls.Add(_rec);
            foreach (var rec in _ls)
            {
                IDictionary<string, object> myUnderlyingObject = rec;
                foreach (var group_field in outputData_)
                {

                    var ruleForThisField = rule_.Where(p => p.OutputFieldId == group_field.Key).ToList();
                    var fieldname = group_field.Key + EV.DOLLAR;
                    foreach (var rule in ruleForThisField)
                    {
                        var rule_fullname = fieldname + rule.Name;
                        try
                        {
                            if (rule.Type == 0)
                            {
                                //var rule_result = rule.ExpValue.FormatWith(rec);
                                //TODO: dòng này xữ lý chậm
                                myUnderlyingObject.Add(rule_fullname, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));
                            }
                            else if (rule.Type == 2)//bool
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.IS(rule.ExpValue.FormatWith(rec)));
                            }
                            else if (rule.Type == 1)//string
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));
                            }
                            else if (rule.Type == 3)//string
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));
                            }
                            else if (rule.Type == 4)//string
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
                            }

                            if (rule == ruleForThisField.Last())
                            {
                                myUnderlyingObject.Add(group_field.First().FieldName, myUnderlyingObject[rule_fullname]);
                            }
                        }
                        catch (Exception ex)
                        {

                            throw new Exception("FAIL at rule:"+rule.Name +"("+rule.ExpValue+")"+Environment.NewLine
                                
                                );
                        }
                        
                    }
                }
            }
            return Newtonsoft.Json.JsonConvert.SerializeObject(_ls);
        }
    }
}