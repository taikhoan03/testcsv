using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using BL;
using Libs;
using System.IO;

namespace FA_admin_site.Controllers
{
    public class outputDataWithName
    {
        public string FieldMapperName { get; set; }
        public string FileMapperName { get; set; }
        public int Id { get; set; }
        public int Order { get; set; }
        public int OutputFieldId { get; set; }
        public int WorkingSetId { get; set; }
        public string FieldName { get; set; }
    }
    public class TestRuleController : Controller
    {
        // GET: TestRule
        public ActionResult Index(int wsid)
        {
            ViewBag.ID = wsid;
            var db = new BL.DA_Model();
            var ws = db.workingSets.Find(wsid);
            var wsi = db.workingSetItems.Where(p => p.WorkingSetId == wsid).ToList();
            // apply rule mapper
            var outputFields = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            var outputData = db.outputDatas.Where(p => outputFields.Any(c => c.Id == p.OutputFieldId) && p.WorkingSetId == ws.Id);

            var outputDataWithName = from p in outputData
                                     join pp in outputFields
                                     on p.OutputFieldId equals pp.Id
                                     select new outputDataWithName
                                     {
                                         FieldMapperName = p.FieldMapperName,
                                         FileMapperName = p.FileMapperName,
                                         Id = p.Id,
                                         Order = p.Order,
                                         OutputFieldId = p.OutputFieldId,
                                         WorkingSetId = p.WorkingSetId,
                                         FieldName = pp.Name
                                     };
            var rulesF = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId && p.WorkingSetId == wsid).ToList();//.OrderBy(p => p.Order);
            var rules = new List<FieldRule>();// db.fieldRules.Where(p => p.WorkingSetItemId == fileid).OrderBy(p => p.Order).ToList();
            foreach (var item in wsi)
            {
                var rules_eachFile= db.fieldRules.Where(p => p.WorkingSetItemId == item.Id).OrderBy(p => p.Order).ToList();
                foreach (var r in rules_eachFile)
                {
                    //foreach (var h in r.ExpValue.GetPlaceHolderName_ExpandObject())
                    //{
                    //    r.ExpValue = r.ExpValue.Replace("{" + h + "}", "{" + item.Filename + ":" + h + "}");
                    //}
                    rules.Add(r);
                }
            }
            foreach (var item in rulesF)
            {
                rules.Add(new FieldRule
                {
                    ExpValue=item.ExpValue
                });
            }
            var seq1Name = "seq1";
            var seq2Name = "seq2";
            var outputDataWithNameList = outputDataWithName.ToList();
            outputDataWithNameList.Add(new outputDataWithName
            {
                FieldMapperName = seq1Name,
                FileMapperName = seq1Name,
                Id = 0,
                Order = 999,
                OutputFieldId = -1,
                WorkingSetId = ws.Id,
                FieldName = seq1Name
            });
            outputDataWithNameList.Add(new outputDataWithName
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
            ViewBag.OutputFields = outputDataWithNameList.GroupBy(p=>p.FieldName).Select(p=>p.First()).ToList();
            var concatAll = string.Join(" ", filteredRules.Select(p => p.ExpValue));
            //foreach (var rule in rule_)
            //{
            //    rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            //}


            var fields = concatAll.GetPlaceHolderName_ExpandObject().Distinct();
            return View(fields);
        }
        [HttpPost]
        public string getOutputInfo(int id,int wsid)
        {
            if (id == -1)
            {
                return null;
            }
            var db = new BL.DA_Model();
            // apply rule mapper
            var outputFields = db.outputFields.FirstOrDefault(p => p.Id==id);


            //var ws = db.workingSets.Find(wsid);
            //var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == wsid);
            //// apply rule mapper
            //var outputFields_ = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            //var outputData = db.outputDatas.Where(p => outputFields_.Any(c => c.Id == p.OutputFieldId) && p.WorkingSetId == ws.Id);

            //var outputDataWithName = from p in outputData
            //                         join pp in outputFields_
            //                         on p.OutputFieldId equals pp.Id
            //                         select new
            //                         {
            //                             FieldMapperName = p.FieldMapperName,
            //                             FileMapperName = p.FileMapperName,
            //                             Id = p.Id,
            //                             Order = p.Order,
            //                             OutputFieldId = p.OutputFieldId,
            //                             WorkingSetId = p.WorkingSetId,
            //                             FieldName = pp.Name
            //                         };
            //var rules = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId && p.WorkingSetId == wsid).ToList();//.OrderBy(p => p.Order);
            
            //var outputDataWithNameList = outputDataWithName.ToList();

            //var outputData_ = outputDataWithNameList.Where(p => p.OutputFieldId == id).GroupBy(c => c.OutputFieldId);
            //var rule_ = rules.ToList();
            //var fieldNeeded = new List<string>();
            //foreach (var group_field in outputData_)
            //{
            //    var ruleForThisField = rule_.Where(p => p.OutputFieldId == group_field.Key).ToList();
            //    foreach(var item in ruleForThisField)
            //    {
            //        foreach(var ph in item.ExpValue.GetPlaceHolderName_ExpandObject())
            //        {
            //            fieldNeeded.Add(ph.Replace(".", "_").Replace(":", "_"));
            //        }
            //    }
            //}

            return Newtonsoft.Json.JsonConvert.SerializeObject(new {
                Decimal = outputFields.Decimal,
                Id = outputFields.Id,
                Length = outputFields.Length,
                Name = outputFields.Name,
                Order = outputFields.Order,
                OutputMapperId = outputFields.OutputMapperId,
                Type = outputFields.Type,
                //Fields = fieldNeeded,
            });
        }
        public class pairNameValue
        {
            public string name { get; set; }
            public string val { get; set; }
        }
        [HttpPost]
        public string runTestRules(int wsid, List<pairNameValue> fields,int outputFieldId=-1,bool includeAllSubRule=false)
        {
            var db = new BL.DA_Model();
            var ws = db.workingSets.Find(wsid);
            var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == wsid);
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
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR); ;//rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }

            foreach (var field in fields)
            {
                field.name = field.name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);//field.name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }


            var dyna = new DynaExp();
            var dt = new System.Data.DataTable();
            //create record for test
            var _ls = new List<Dictionary<string, object>>();
            var _lsNoName= new List<Dictionary<string, object>>();
            var _rec = new Dictionary<string, object>();
            var _recNoName = new Dictionary<string, object>();
            foreach (var item in fields.Where(p => p.name.IndexOf(EV.DOT) >= 0))
            {
                _rec.Add(item.name, item.val==null?"":item.val);
            }
            foreach (var item in fields.Where(p=>p.name.IndexOf(EV.DOT)<0))
            {
                _recNoName.Add(item.name, item.val == null ? "" : item.val);
            }
            _ls.Add(_rec);
            _lsNoName.Add(_recNoName);
            var ls_rs= Process_final3(firstFileId.Id, _lsNoName, limit: 10, addSequence: false, applyRules: true);
            foreach (var item in ls_rs)
            {
                foreach (var dic in item)
	            {
                    _ls[0].Add(dic.Key,dic.Value);
	            }
            }
            var rs = new List<Dictionary<string, object>>();
            var rs_obj = new Dictionary<string, object>();
            
            foreach (var rec in _ls)
            {
                IDictionary<string, object> myUnderlyingObject = rec;
                IDictionary<string, object> rs_under = rs_obj;
                if (includeAllSubRule)
                    rs_obj = rec;
                foreach (var group_field in outputData_)
                {

                    var ruleForThisField = rule_.Where(p => p.OutputFieldId == group_field.Key).ToList();
                    if (outputFieldId > 0)
                        ruleForThisField = ruleForThisField.Where(p => p.OutputFieldId == outputFieldId).ToList();
                    var fieldname = group_field.First().OutputFieldId + EV.DOLLAR;
                    foreach (var rule in ruleForThisField)
                    {
                        var rule_fullname = fieldname + rule.Name;
                        try
                        {
                            if (rule.Type == 0)
                            {
                                //var rule_result = rule.ExpValue.FormatWith(rec);
                                //TODO: dòng này xữ lý chậm
                                //var ruleUpdate= myUnderlyingObject.FirstOrDefault(p=>p.Key.Contains("RuleExp_"+p.))
                                //myUnderlyingObject.Add(rule_fullname, 
                                //    new
                                //    {
                                //        exp=,
                                //        value=
                                //    });// target.Eval(rule_result));
                                myUnderlyingObject.Add(rule_fullname, dt.Compute(rule.ExpValue.FormatWith(rec), ""));
                                rec.Add(rule_fullname + "RuleExp_", rule.ExpValue);
                                rs_under.Add(rule_fullname, dt.Compute(rule.ExpValue.FormatWith(rec), ""));
                                rs_under.Add(rule_fullname + "RuleExp_", rule.ExpValue);
                                /*dt.Compute(rule.ExpValue.FormatWith(dicrec), "")*/
                                ;
                            }
                            else if (rule.Type == 2)//bool
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.IS(rule.ExpValue.FormatWith(rec)));
                                rec.Add(rule_fullname + "RuleExp_", rule.ExpValue);

                                rs_under.Add(rule_fullname, dyna.IS(rule.ExpValue.FormatWith(rec)));
                                rs_under.Add(rule_fullname + "RuleExp_", rule.ExpValue);
                            }
                            else if (rule.Type == 1)//string
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));
                                rec.Add(rule_fullname + "RuleExp_", rule.ExpValue);

                                rs_under.Add(rule_fullname, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));
                                rs_under.Add(rule_fullname + "RuleExp_", rule.ExpValue);
                            }
                            else if (rule.Type == 3)//string
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));
                                rec.Add(rule_fullname + "RuleExp_", rule.ExpValue);

                                rs_under.Add(rule_fullname, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));
                                rs_under.Add(rule_fullname + "RuleExp_", rule.ExpValue);
                            }
                            else if (rule.Type == 4)//string
                            {
                                myUnderlyingObject.Add(rule_fullname, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
                                rec.Add(rule_fullname + "RuleExp_", rule.ExpValue);

                                rs_under.Add(rule_fullname, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
                                rs_under.Add(rule_fullname + "RuleExp_", rule.ExpValue);
                            }

                            if (rule == ruleForThisField.Last())
                            {
                                myUnderlyingObject.Add(group_field.First().FieldName, myUnderlyingObject[rule_fullname]);

                                rs_under.Add(group_field.First().FieldName, myUnderlyingObject[rule_fullname]);
                                rs.Add(rs_obj);
                            }
                        }
                        catch (Exception ex)
                        {
                            rs_under.Add(rule_fullname, "FAIL at rule:" + rule.Name + "(" + rule.ExpValue + ")");
                            rs.Add(rs_obj);
                            //throw new Exception("FAIL at rule:"+rule.Name +"("+rule.ExpValue+")"+Environment.NewLine

                            //    );
                        }
                        
                    }
                }
            }
            
            //foreach (var rec in ls_rs)
            //{
            //    var arrName = new List<string>();
            //    foreach (var pro in rec)
            //    {
            //        arrName.Add(pro.Key);
            //    }
            //    foreach (var item in arrName)
            //    {
            //        rec[item] =new
            //        {
            //            exp=
            //        }
            //    }
            //}
            return Newtonsoft.Json.JsonConvert.SerializeObject(rs);
        }

        private IEnumerable<Dictionary<string, object>> Process_final3(int fileid,List<Dictionary<string,object>> lsSample, decimal limit = 2 * 1000 * 1000 * 1000, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
        {
            //var data = Process_final2(fileid, limit);
            //var tmpRs = new List<Dictionary<string, object>>();
            //foreach (var line in data.Data)
            //{
            //    var dic = new Dictionary<string, object>();
            //    var i = 0;
            //    foreach (var col in data.Name_index)
            //    {
            //        dic.Add(col.Key, line[i]);
            //        i++;
            //    }
            //    tmpRs.Add(dic);
            //}
            //GC.Collect();
            //return tmpRs;
            //int limit = 100;
            //const string tab = "\t";
            var sorted_file1 = Enumerable.Empty<Dictionary<string, object>>().OrderBy(x => 1);
            using (var db = new DA_Model())
            {
                var wsFile = db.workingSetItems.FirstOrDefault(p => p.Id == fileid);
                var ws = db.workingSets.FirstOrDefault(p => p.Id == wsFile.WorkingSetId);

                var sortAndActions = db.fieldOrderAndActions.Where(p => p.WorkingSetItemId == fileid).Select(p => new
                {
                    ConcatenateWithDelimiter = p.ConcatenateWithDelimiter,
                    DuplicatedAction = p.DuplicatedAction,
                    DuplicatedActionType = p.DuplicatedActionType,
                    FieldName = p.FieldName,
                    Id = p.Id,
                    Order = p.Order,
                    OrderType = p.OrderType,
                    WorkingSetItemId = p.WorkingSetItemId,
                    FileName = wsFile.Filename,
                    Delimiter = p.ConcatenateWithDelimiter
                }).ToList();

                var fields_sort = sortAndActions.OrderBy(x => x.Order)
                    .ToDictionary(x => x.FieldName, x => new SortField//.Replace(".", EV.DOT) + EV.DOLLAR + x.FieldName
                    {
                        name = x.FieldName,
                        duplicateAction = (DuplicateAction)x.DuplicatedAction,
                        sortType = (SortType)x.OrderType,
                        duplicateActionType = x.DuplicatedActionType,
                        delimiter = x.Delimiter
                    }

                    );
                var fieldTypes = db.jobFileLayouts.Where(p => p.WorkingSetItemId == fileid).ToDictionary(p => p.Fieldname, p => p.Type);
                //var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                //                        Config.Data.GetKey("input_folder_process"),
                //                        ws.State,
                //                        ws.County
                //                        );
                //path = path + @"\" + wsFile.Filename;
                //var test= Helpers.ReadCSV.ReadAsArray(wsFile.Filename, path, limit);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    //test.get("EXEMPTION@csv$EXEMPTION_DSCR", test.Data[3000000]);
                //    test.get("EXEMPTION@csv$EXEMPTION_DSCR", 3000000);
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                //var tab = KnownDelimeter(path);
                var file1 = lsSample;// ReadCSV.ReadAsDictionary("", path, limit, tab);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    var oo=file1[3000000]["EXEMPTION@csv$EXEMPTION_DSCR"];
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                var primaryKey =  wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR + wsFile.PrimaryKey.ReplaceUnusedCharacters();
                if (string.IsNullOrEmpty(primaryKey))
                {
                    throw new Exception("No Primary Key, Please select 1 first");
                }


                var allrecs = file1;// new List<Dictionary<string, object>>();

                var sortActions = fields_sort.OrderBy(p => p.Value.duplicateAction);
                var hasKeepAllRows = sortActions.Count(p => p.Value.duplicateAction == DuplicateAction.KeepAllRows) > 0;

                sorted_file1 = allrecs.OrderBy(x => 1);


                //apply rules
                if (applyRules)
                {
                    var rules = db.fieldRules.Where(p => p.WorkingSetItemId == fileid).OrderBy(p => p.Order).ToList();
                    //update rules as part of fieldType
                    foreach (var rule in rules)
                    {
                        fieldTypes.Add(rule.Name, rule.Type);
                    }
                    var lookupTable = db.FACodeTables.ToList();
                    var lookupItems = db.FACodes.ToList();
                    var lookupRule = new BL.LookupRule(lookupTable, lookupItems);
                    //if(allrecs.First().Keys.First().Contains(EV.DOT))
                    //    CallFunction(rules, sorted_file1, "", lookupRule);
                    //else
                        CallFunction(rules, sorted_file1, wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR, lookupRule);
                    //CallFunction(rules, sorted_file1, "", lookupRule);
                    rules = null;

                }
                //allrecs.Clear();
                //allrecs = null;

            }
            //GC.Collect();
            return sorted_file1;
        }
        private static DynaExp dyna = new DynaExp();
        private static void CallFunction(IEnumerable<FieldRule> rules, IOrderedEnumerable<Dictionary<string, object>> sorted_file1, string namePrefix, LookupRule lookupRule)
        {

            using (var dt = new System.Data.DataTable())
            {
                foreach (var rec in sorted_file1)
                {
                    //rec.Add(rule.Name, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));
                    foreach (var rule in rules)
                    {
                        if (rule.Type == 0)//math
                        {
                            rec.Add(rule.Name, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));
                            rec.Add(rule.Name+ "RuleExp_", rule.ExpValue);
                        }
                        else if (rule.Type == 2)//bool
                        {
                            rec.Add(rule.Name, dyna.IS(rule.ExpValue.FormatWith(rec)));
                            rec.Add(rule.Name + "RuleExp_", rule.ExpValue);
                        }
                        else if (rule.Type == 1)//string
                        {
                            rec.Add(rule.Name, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));
                            rec.Add(rule.Name + "RuleExp_", rule.ExpValue);
                        }
                        else if (rule.Type == 3)//FUNC_Num
                        {
                            rec.Add(rule.Name, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));
                            rec.Add(rule.Name + "RuleExp_", rule.ExpValue);
                        }
                        else if (rule.Type == 4)//obj AS_IS/IF
                        {
                            rec.Add(rule.Name, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
                            rec.Add(rule.Name + "RuleExp_", rule.ExpValue);
                        }
                        else if (rule.Type == 6)//obj LOOKUP
                        {
                            //format exp should be: Tablename:LookupKey(Rule_8)
                            var parts = rule.ExpValue.FormatWith(rec).Split(new string[] { "]]" }, StringSplitOptions.None);
                            var tableName = parts[0];
                            var val = parts[1];//parts[1]:Rule_8
                            var rep = "NULL";
                            if (!lookupRule.rules.ContainsKey(tableName)
                                || !lookupRule.rules[tableName].ContainsKey(val))
                                rep = "NULL";
                            else
                                rep = lookupRule.rules[tableName][val];
                            rec.Add(rule.Name, rep);
                            rec.Add(rule.Name + "RuleExp_", rule.ExpValue);
                        }
                    }

                    if (!string.IsNullOrEmpty(namePrefix))
                    {
                        var newrec = new Dictionary<string, object>(rec);
                        rec.Clear();
                        foreach (var item in newrec)
                        {
                            //item = new KeyValuePair<string, object>();
                            rec.Add(namePrefix + item.Key, item.Value);
                        }
                    }

                }
            }
        }
    }
}