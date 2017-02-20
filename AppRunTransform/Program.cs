using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BL;
using Libs;
using System.Data;
using System.Threading;
using System.IO;

namespace AppRunTransform
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("App is running...");
            using (Process p = Process.GetCurrentProcess())
                p.PriorityClass = ProcessPriorityClass.BelowNormal;
            var db = new BL.DA_Model();
            //var a = db.runTransformRequests.Where(p => p.WorkingSetId == 4002);
            while (true)
            {
                //lay req dang chạy trước (trường hợp bị tắt hoặc dừng đột xuất thì chạy lại)
                var req = db.runTransformRequests.FirstOrDefault(p => p.Status == 1 && p.IsReady && !p.IsDeleted);
                if(req==null)
                    req = db.runTransformRequests.FirstOrDefault(p => p.Status == 0 && p.IsReady && !p.IsDeleted);
                if (req != null) {
                    Console.WriteLine("processing ws: " + req.WorkingSetId);
                    req.Status = 1;
                    req.IsReady = true;
                    db.SaveChanges();
                    try
                    {
                        var watch = Stopwatch.StartNew();
                        var outputname=testMap(req.WorkingSetId, true);
                        watch.Stop();
                        req.TimeCost = Convert.ToInt32(watch.Elapsed.TotalSeconds);
                        req.Status = 2;
                        req.Detail = "";
                        req.OutputName = outputname;
                        db.SaveChanges();
                        continue;
                    }
                    catch (Exception ex)
                    {
                        req.Status = 3;//fail
                        req.Detail = ex.Message + Environment.NewLine + ex.StackTrace;
                        db.SaveChanges();
                    }
                }
                
                
                ////check has any next req
                //var next= db.runTransformRequests.FirstOrDefault(p => p.Status == 0 && p.IsReady && !p.IsDeleted);
                //if (next == null)
                //{
                //    Console.WriteLine("No more request, waiting...");
                //    Thread.Sleep(10 * 1000);
                //}
                Console.WriteLine("No more request, waiting...");
                Thread.Sleep(10 * 1000);

            }
        }
        public static string testMap(int id, bool cleanUpResult = false)
        {
            
            
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id);
            //if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            var dic = new Dictionary<string, IEnumerable<IDictionary<string, object>>>();
            var limit = 2 * 1000 * 1000 * 1000;


            var files = new List<int>();
            if (linkageData != null)
            {
                foreach (var item in linkageData)
                {
                    if (files.FirstOrDefault(p => p == item.firstId) == 0)
                    {
                        files.Add(item.firstId);
                    }
                    if (files.FirstOrDefault(p => p == item.sndId) == 0)
                    {
                        files.Add(item.sndId);
                    }
                }
            }

            var ls = new List<IEnumerable<IDictionary<string, object>>>();
            var _ls = new List<IDictionary<string, object>>();
            var allRec = new List<IDictionary<string, object>>();


            var dtAll = new DataTable();
            if (linkageData != null)
            {
                var groupLinkageData = linkageData.GroupBy(p => p.firstId + p.sndId);
                string key = groupLinkageData.First().First().firstField;
                string sndKey = groupLinkageData.First().Last().firstField;
                foreach (var item in groupLinkageData)
                {
                    var FF_result = new List<IDictionary<string, object>>();
                    var loadF1 = Process_final(item.First().firstId, limit: limit, addSequence: false, applyRules: false);

                    var loadF2 = Process_final(item.First().sndId, limit: limit, addSequence: false, applyRules: false);

                    var firstF1 = loadF1.First();
                    var firstF2 = loadF2.First();



                    //missing field nếu ko join dc
                    //var ff = from p in loadF1
                    //         join pp in loadF2
                    //         on new { a = p[item.First().firstField], b = p[item.Last().firstField] } equals new { a = pp[item.First().sndField], b = pp[item.Last().sndField] }
                    //         into ps
                    //         from g in ps//.DefaultIfEmpty()
                    //         select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g);// g.Where(kvp => !p.ContainsKey(kvp.Key)));//
                    if (_ls.Count == 0)
                    {
                        _ls = loadF1.ToList();
                    }
                    var left1 = item.First().firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.First().firstField;
                    var right1 = item.Last().firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().firstField;
                    var left2 = item.First().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.First().sndField;
                    var right2 = item.Last().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().sndField;
                    var ff = from p in _ls
                             join pp in loadF2
                             on new
                             {
                                 a = p[left1].ToString(),
                                 b = p[right1].ToString()
                             }
                             equals new
                             {
                                 a = pp[left2].ToString(),
                                 b = pp[right2].ToString()
                             }
                             into ps
                             from g in ps//.DefaultIfEmpty()
                             select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g).ToDictionary(x => x.Key, x => x.Value);
                    //var ff = from p in _ls
                    //         join pp in loadF2
                    //         on new
                    //         {
                    //             a = p[item.First().firstFilename + EV.DOLLAR + item.First().firstField],
                    //             b = p[item.Last().firstFilename + EV.DOLLAR + item.Last().firstField]
                    //         }
                    //         equals new
                    //         {
                    //             a = pp[item.First().sndFilename + EV.DOLLAR + item.First().sndField],
                    //             b = pp[item.Last().sndFilename + EV.DOLLAR + item.Last().sndField]
                    //         }
                    //         into ps
                    //         from g in ps//.DefaultIfEmpty()
                    //         select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g).ToDictionary(x=>x.Key,x=>x.Value);
                    _ls = new List<IDictionary<string, object>>(ff);// ff.ToDictionary(x=>x.Keys).ToList();

                }
            }
            else
            {
                var loadF1 = Process_final(firstFileId.Id, limit: limit, addSequence: false, applyRules: false);
                _ls = loadF1.ToList();// new List<IDictionary<string, object>>(loadF1);
            }

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
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId && p.WorkingSetId == id).ToList();//.OrderBy(p => p.Order);
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
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }


            var dyna = new DynaExp();
            var dt = new System.Data.DataTable();
            foreach (var rec in _ls)
            {
                IDictionary<string, object> myUnderlyingObject = rec;
                foreach (var group_field in outputData_)
                {

                    var ruleForThisField = rule_.Where(p => p.OutputFieldId == group_field.Key).ToList();
                    var fieldname = group_field.Key + EV.DOLLAR;
                    var mapper = group_field.First();
                    //TODO: nếu ko viết Rule, và chỉ có 1 field dc chọn để map
                    if(mapper.FieldMapperName!=seq1Name && mapper.FieldMapperName != seq2Name)
                    if (ruleForThisField.Count==0)
                    {
                        if (group_field.Count() == 1 && !string.IsNullOrEmpty(mapper.FieldMapperName))
                        {
                            
                            var _name = mapper.FileMapperName +":"+ mapper.FieldMapperName;
                            _name=_name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
                            myUnderlyingObject.Add(mapper.FieldName, myUnderlyingObject[_name]);
                        }
                        
                    }
                    foreach (var rule in ruleForThisField)
                    {
                        try
                        {
                            var rule_fullname = fieldname + rule.Name;
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
                                myUnderlyingObject.Add(mapper.FieldName, myUnderlyingObject[rule_fullname]);
                            }
                        }
                        catch (Exception ex)
                        {

                            throw new Exception("Fail to run Rule:" + rule.ExpValue + Environment.NewLine +
                                " rec: " + Newtonsoft.Json.JsonConvert.SerializeObject(rec) + Environment.NewLine +
                                " Message:" + ex.Message
                                );
                        }
                    }
                }
            }

            var a = 1;
            string primaryKey = "";
            if (linkageData != null)
            {
                var firstLinkage = linkageData.First();
                primaryKey = firstLinkage.firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + firstLinkage.firstField;
            }else
            {
                primaryKey = firstFileId.Filename.Replace(".", EV.DOT) + EV.DOLLAR + firstFileId.PrimaryKey;
            }
            

            var group1 = _ls.ToList().GroupBy(p => p[primaryKey]);

            //add sequence
            foreach (var _group in group1)
            {
                var increasement = 1;
                foreach (var record in _group)
                {
                    record.Add(seq1Name, 1);
                    record.Add(seq2Name, increasement);
                    increasement++;
                }

            }


            dtAll = Ulti.ToDataTable(_ls);
            //remove columns
            if (cleanUpResult)
            {
                var list_col_to_remove = new List<DataColumn>();
                foreach (DataColumn col in dtAll.Columns)
                {
                    if (!outputData_.Any(c => c.First().FieldName == col.ColumnName))
                    {
                        list_col_to_remove.Add(col);
                    }
                }
                foreach (var col in list_col_to_remove)
                {
                    dtAll.Columns.Remove(col);
                }
            }
            //format, and length

            var colFields = new List<string>();
            foreach (DataColumn item in dtAll.Columns)
            {
                if (item.ColumnName != seq1Name && item.ColumnName != seq2Name)
                    colFields.Add(item.ColumnName);
            }
            var outputDic = outputFields.Where(c => colFields.Any(d => d == c.Name)).ToList().ToDictionary(x => x.Name, x => x);
            foreach (DataRow row in dtAll.Rows)
            {
                foreach (var col in colFields)
                {
                    var formatCell = outputDic[col];
                    if (formatCell.Type == EV.TYPE_NUM)
                    {
                        var cell = row[col];
                        cell = Math.Round(Convert.ToDecimal(cell), formatCell.Decimal);
                    }
                    else
                    {
                        var cell = row[col];
                        var content = cell.ToString();
                        if (!string.IsNullOrEmpty(content) && content.Length >= formatCell.Length)
                        {
                            row[col] = content.Substring(0, formatCell.Length);
                        }
                    }
                }
            }
            var fileOutput = db.outputMappers.Find(ws.SeletedOutputId);
            var name = DateTime.Now.ToString("yyyyMMdd") + "_" + fileOutput.Name + "_" + ws.User+".csv";
            Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
                ws.State+"\\"+ws.County+"\\"+ name, dtAll);
            _ls.Clear();
            dtAll.Clear();
            dtAll.Dispose();
            GC.Collect();
            return name;
        }
        private static IEnumerable<IDictionary<string, object>> Process_final(int fileid, decimal limit = 100, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
        {
            //int limit = 100;
            var tab = "\t";
            var db = new BL.DA_Model();

            //var wsFiles = db.workingSetItems.Where(p => p.Id == fileid);
            var wsFile = db.workingSetItems.FirstOrDefault(p => p.Id == fileid);
            var ws = db.workingSets.FirstOrDefault(p => p.Id == wsFile.WorkingSetId);

            var sortAndActions = db.fieldOrderAndActions.Where(p => p.WorkingSetItemId == fileid).Select(p => new {
                ConcatenateWithDelimiter = p.ConcatenateWithDelimiter,
                DuplicatedAction = p.DuplicatedAction,
                DuplicatedActionType = p.DuplicatedActionType,
                FieldName = p.FieldName,
                Id = p.Id,
                Order = p.Order,
                OrderType = p.OrderType,
                WorkingSetItemId = p.WorkingSetItemId,
                FileName = wsFile.Filename
            });
            //from p in db.fieldOrderAndActions.Where(p => p.WorkingSetItemId == fileid)
            //                 join pp in wsFiles
            //                 on p.WorkingSetItemId equals pp.Id
            //                 select new
            //                 {
            //                     ConcatenateWithDelimiter=p.ConcatenateWithDelimiter,
            //                     DuplicatedAction= p.DuplicatedAction,
            //                     DuplicatedActionType=p.DuplicatedActionType,
            //                     FieldName= p.FieldName,
            //                     Id=p.Id,
            //                     Order=p.Order,
            //                     OrderType=p.OrderType,
            //                     WorkingSetItemId=p.WorkingSetItemId,
            //                     FileName=pp.Filename
            //                 }
            //                 ;
            var fields_sort = sortAndActions.OrderBy(x => x.Order)
                .ToDictionary(x => x.FileName.Replace(".", EV.DOT) + EV.DOLLAR + x.FieldName, x => new SortField
                {
                    name = x.FieldName,
                    duplicateAction = (DuplicateAction)x.DuplicatedAction,
                    sortType = (SortType)x.OrderType,
                    duplicateActionType = x.DuplicatedActionType
                }
                //.Select(x => new SortField
                //{
                //    name = x.FieldName,
                //    duplicateAction = (DuplicateAction)x.DuplicatedAction,
                //    sortType = (SortType)x.OrderType,
                //    duplicateActionType = x.DuplicatedActionType
                //}
                );//.ToDictionary<string, SortField>(x=>x.fie);
            var fieldTypes = db.jobFileLayouts.Where(p => p.WorkingSetItemId == fileid).ToDictionary(p => p.Fieldname, p => p.Type);
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    ws.State,
                                    ws.County
                                    );
            path = path + @"\" + wsFile.Filename;
            var file1 = Helpers.ReadCSV.ReadAsDictionary(wsFile.Filename, path, limit);
            var primaryKey = wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR + wsFile.PrimaryKey.ReplaceUnusedCharacters();
            if (string.IsNullOrEmpty(primaryKey))
            {
                throw new Exception("No Primary Key, Please select 1 first");
            }

            var group1 = file1.ToList().GroupBy(p => p[primaryKey]);

            var allrecs = new List<IDictionary<string, object>>();

            var sortActions = fields_sort.OrderBy(p => p.Value.duplicateAction);
            foreach (var _group in group1)
            {
                var breakOtherRecords = false;
                var ignoreAll = false;
                var record = _group.FirstOrDefault();
                var isResponseWithError = false;
                var hasKeepAllRows = sortActions.Where(p => p.Value.duplicateAction == DuplicateAction.KeepAllRows).Count() > 0;
                foreach (var sortField in sortActions)
                {
                    var action = sortField.Value.duplicateAction;

                    try
                    {

                        if (action == DuplicateAction.ResponseWithError)
                        {
                            //throw new Exception("ResponseWithError");
                            isResponseWithError = true;
                        }
                        else if (action == DuplicateAction.PickupFirstValue)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            var v = _group.FirstOrDefault()[sortField.Key];
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }

                        }
                        else if (action == DuplicateAction.PickupLastValue)
                        {
                            if (!hasKeepAllRows) { breakOtherRecords = true; }
                            var v = _group.LastOrDefault()[sortField.Key];
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }

                        }
                        else if (action == DuplicateAction.PickupFirstUn_NULL_value)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            var v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.PickupMaximumValue)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            var v = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.PickupMinimumValue)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            var v = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.SumAllRow)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            var v = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        //TODO: ConcatenateWithDelimiter phải xác định delimeter
                        else if (action == DuplicateAction.ConcatenateWithDelimiter)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;

                            var v = string.Join(",", _group.Select(i => i[sortField.Key]));
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.KeepAllRows)
                        {
                            breakOtherRecords = false;

                        }
                    }
                    catch (Exception ex)
                    {

                        throw new Exception("FAIL at: " + sortField.Key
                            + ", sortType: " + action + Environment.NewLine + "Record: " + Environment.NewLine +
                            Newtonsoft.Json.JsonConvert.SerializeObject(_group, Newtonsoft.Json.Formatting.Indented));
                    }
                }
                foreach (var rec in _group)
                {

                    if (ignoreAll)
                        break;

                    rec.Add(wsFile.Filename + EV.DOLLAR + "isDuplicated", 0);// = 0;
                    var numOfPrimaryKeyFound = _group.Count();
                    rec.Add(wsFile.Filename + EV.DOLLAR + "numOfPrimaryKeyFound", numOfPrimaryKeyFound);
                    if (numOfPrimaryKeyFound > 1)
                    {
                        if (isResponseWithError)
                            throw new Exception("ResponseWithError");
                        rec[wsFile.Filename + EV.DOLLAR + "isDuplicated"] = 1;

                    }

                    allrecs.Add(rec);

                    if (breakOtherRecords)
                    {

                        break;
                    }

                }

            }

            //Sorting

            var sorted_file1 = Enumerable.Empty<IDictionary<string, object>>().OrderBy(x => 1);
            var sortFieldsNotNONE = fields_sort.Where(p => p.Value.sortType != SortType.None);
            var firstOrderItem = sortFieldsNotNONE.FirstOrDefault().Value;
            if (firstOrderItem != null)
            {
                if (firstOrderItem.sortType == SortType.Asccending)
                {
                    if (fieldTypes.ContainsKey(firstOrderItem.name))
                    {
                        if (fieldTypes[firstOrderItem.name] == 0)//int
                        {
                            sorted_file1 = allrecs.OrderBy(x => Convert.ToDecimal(x[firstOrderItem.name]));
                        }
                        else
                        {
                            sorted_file1 = allrecs.OrderBy(x => x[firstOrderItem.name].ToString());
                        }
                    }
                }
                else if (firstOrderItem.sortType == SortType.Deccending)
                {
                    if (fieldTypes.ContainsKey(firstOrderItem.name))
                    {
                        if (fieldTypes[firstOrderItem.name] == 0)//int
                        {
                            sorted_file1 = allrecs.OrderByDescending(x => Convert.ToDecimal(x[firstOrderItem.name]));
                        }
                        else
                        {
                            sorted_file1 = allrecs.OrderByDescending(x => x[firstOrderItem.name].ToString());
                        }
                    }
                }


                foreach (var item in sortFieldsNotNONE.Skip(1))
                {
                    if (item.Value.sortType == SortType.Asccending)
                    {
                        if (fieldTypes.ContainsKey(item.Key))
                        {
                            if (fieldTypes[item.Key] == 0)//int
                            {
                                sorted_file1 = sorted_file1.ThenBy(x => Convert.ToDecimal((decimal)x[item.Key]));
                            }
                            else
                            {
                                sorted_file1 = sorted_file1.ThenBy(x => x[item.Key].ToString());
                            }
                        }

                    }
                    else if (item.Value.sortType == SortType.Deccending)
                    {
                        if (fieldTypes.ContainsKey(item.Key))
                        {
                            if (fieldTypes[item.Key] == 0)//int
                            {
                                sorted_file1 = sorted_file1.ThenByDescending(x => Convert.ToDecimal((decimal)x[item.Key]));
                            }
                            else
                            {
                                sorted_file1 = sorted_file1.ThenByDescending(x => x[item.Key].ToString());
                            }
                        }

                    }
                }
            }
            else
            {
                sorted_file1 = allrecs.OrderBy(x => 1);
            }


            group1 = sorted_file1.ToList().GroupBy(p => p[primaryKey]);
            //add sequence
            if (addSequence)
                foreach (var _group in group1)
                {
                    var increasement = 1;
                    foreach (var record in _group)
                    {
                        record.Add("seq", 1);
                        record.Add("seq2", increasement);
                        increasement++;
                    }

                }


            //apply rules
            if (applyRules)
            {
                var rules = db.fieldRules.Where(p => p.WorkingSetItemId == fileid).OrderBy(p => p.Order);
                //update rules as part of fieldType
                foreach (var rule in rules)
                {
                    fieldTypes.Add(rule.Name, rule.Type);
                }
                CallFunction(rules, sorted_file1);

                //var dyna = new DynaExp();
                //var dt = new System.Data.DataTable();
                //foreach (var rule in rules)
                //{
                //    if (rule.Type == 0)
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            IDictionary<string, object> myUnderlyingObject = rec;
                //            try
                //            {
                //                var rule_result = rule.ExpValue.FormatWith(rec);
                //                //TODO: dòng này xữ lý chậm
                //                myUnderlyingObject.Add(rule.Name, dt.Compute(rule_result, ""));// target.Eval(rule_result));

                //            }
                //            catch (Exception ex)
                //            {

                //                throw new Exception("Process final --- Fail to run Rule:" + rule.ExpValue + Environment.NewLine +
                //                        " rec: " + Newtonsoft.Json.JsonConvert.SerializeObject(rec) + Environment.NewLine +
                //                        " Message:" + ex.Message
                //                        );
                //            }


                //        }
                //    }
                //    else if (rule.Type == 2)//bool
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            IDictionary<string, object> myUnderlyingObject = rec;
                //            try
                //            {
                //                var rule_result = dyna.IS(rule.ExpValue.FormatWith(rec));
                //                //TODO: dòng này xữ lý chậm
                //                myUnderlyingObject.Add(rule.Name, rule_result);
                //            }
                //            catch (Exception ex)
                //            {

                //                throw new Exception("Process final --- Fail to run Rule:" + rule.ExpValue + Environment.NewLine +
                //                        " rec: " + Newtonsoft.Json.JsonConvert.SerializeObject(rec) + Environment.NewLine +
                //                        " Message:" + ex.Message
                //                        );
                //            }



                //        }
                //    }
                //    else if (rule.Type == 1)//string
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            IDictionary<string, object> myUnderlyingObject = rec;
                //            try
                //            {
                //                var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
                //                //TODO: dòng này xữ lý chậm
                //                myUnderlyingObject.Add(rule.Name, rule_result);
                //            }
                //            catch (Exception ex)
                //            {

                //                throw new Exception("Process final --- Fail to run Rule:" + rule.ExpValue + Environment.NewLine +
                //                        " rec: " + Newtonsoft.Json.JsonConvert.SerializeObject(rec) + Environment.NewLine +
                //                        " Message:" + ex.Message
                //                        );
                //            }



                //        }
                //    }
                //    //try
                //    //{

                //    //}
                //    //catch (Exception ex)
                //    //{

                //    //    throw new Exception("Process final --- Fail to run Rule:" + rule.ExpValue + Environment.NewLine +
                //    //            " rec: " + Newtonsoft.Json.JsonConvert.SerializeObject(rec) + Environment.NewLine +
                //    //            " Message:" + ex.Message
                //    //            );
                //    //}

                //}
            }

            return sorted_file1;
        }

        public enum DuplicateAction
        {
            ResponseWithError = 0,
            KeepAllRows = 1,
            PickupFirstValue = 2,
            PickupLastValue = 3,
            PickupFirstUn_NULL_value = 4,
            PickupMaximumValue = 5,
            PickupMinimumValue = 6,
            SumAllRow = 7,
            ConcatenateWithDelimiter = 8,
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

        private static DynaExp dyna = new DynaExp();
        private static void CallFunction(IOrderedQueryable<FieldRule> rules, IOrderedEnumerable<IDictionary<string, object>> sorted_file1)
        {
            
            var dt = new System.Data.DataTable();
            foreach (var rule in rules)
            {
                if (rule.Type == 0)//math
                {
                    foreach (var rec in sorted_file1)
                    {
                        IDictionary<string, object> myUnderlyingObject = rec;
                        var rule_result = rule.ExpValue.FormatWith(rec);
                        //TODO: dòng này xữ lý chậm
                        myUnderlyingObject.Add(rule.Name, dt.Compute(rule_result, ""));// target.Eval(rule_result));


                    }
                }
                else if (rule.Type == 2)//bool
                {
                    foreach (var rec in sorted_file1)
                    {
                        IDictionary<string, object> myUnderlyingObject = rec;
                        var rule_result = dyna.IS(rule.ExpValue.FormatWith(rec));
                        //TODO: dòng này xữ lý chậm
                        myUnderlyingObject.Add(rule.Name, rule_result);


                    }
                }
                else if (rule.Type == 1)//string
                {
                    foreach (var rec in sorted_file1)
                    {
                        IDictionary<string, object> myUnderlyingObject = rec;
                        var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
                        //TODO: dòng này xữ lý chậm
                        myUnderlyingObject.Add(rule.Name, rule_result);


                    }
                }
                else if (rule.Type == 3)//FUNC_Num
                {
                    foreach (var rec in sorted_file1)
                    {
                        IDictionary<string, object> myUnderlyingObject = rec;
                        var rule_result = dyna.FUNC_Num(rule.ExpValue.FormatWith(rec));
                        //TODO: dòng này xữ lý chậm
                        myUnderlyingObject.Add(rule.Name, rule_result);


                    }
                }
                else if (rule.Type == 4)//obj AS_IS/IF
                {
                    foreach (var rec in sorted_file1)
                    {
                        IDictionary<string, object> myUnderlyingObject = rec;
                        var rule_result = dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec));
                        //TODO: dòng này xữ lý chậm
                        myUnderlyingObject.Add(rule.Name, rule_result);


                    }
                }

            }
        }
    }
}
