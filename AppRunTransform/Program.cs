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
            //using (Process p = Process.GetCurrentProcess())
            //    p.PriorityClass = ProcessPriorityClass.BelowNormal;
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
                    finally
                    {
                        GC.Collect();
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
            var limit = 200000;// 2 * 1000 * 1000 * 1000;


            //var files = new List<int>();
            //if (linkageData != null)
            //{
            //    foreach (var item in linkageData)
            //    {
            //        if (files.FirstOrDefault(p => p == item.firstId) == 0)
            //        {
            //            files.Add(item.firstId);
            //        }
            //        if (files.FirstOrDefault(p => p == item.sndId) == 0)
            //        {
            //            files.Add(item.sndId);
            //        }
            //    }
            //}
            //all recs
            var all_rec = new List<IDictionary<string, object>>();


            var dtAll = new DataTable();
            var numOfRun = 0;
            var cached1 = Enumerable.Empty<IDictionary<string, object>>();
            var cached2 = Enumerable.Empty<IDictionary<string, object>>();
            var loadF1 = Enumerable.Empty<IDictionary<string, object>>();
            var loadF2 = Enumerable.Empty<IDictionary<string, object>>();
            //declare RuleMapper 
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
            
            ////END declare RuleMapper 
            //nạp dữ liệu vào all_rec
            if (linkageData != null)
            {
                var groupLinkageData = linkageData.GroupBy(p => p.firstId + p.sndId);
                var key = groupLinkageData.First().First().firstField;
                var sndKey = groupLinkageData.First().Last().firstField;
                foreach (var item in groupLinkageData)
                {
                    //var FF_result = new List<IDictionary<string, object>>();
                     //Process_final(item.First().firstId, limit: limit, addSequence: false, applyRules: false);

                     //Process_final(item.First().sndId, limit: limit, addSequence: false, applyRules: false);
                    if (numOfRun == 0)
                    {
                        Console.WriteLine("Get data from file " + item.First().firstFilename);
                        loadF1 = Process_final(item.First().firstId, limit: limit, addSequence: false, applyRules: false);
                        GC.Collect();
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final(item.First().sndId, limit: limit, addSequence: false, applyRules: false);
                        GC.Collect();
                        cached1 = loadF1;
                        cached2 = loadF2;
                        all_rec = loadF1.ToList();
                    }
                    else
                    {
                        loadF1 = cached2;
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final(item.First().sndId, limit: limit, addSequence: false, applyRules: false);
                    }
                    GC.Collect();
                    numOfRun++;
                    var firstF1 = loadF1.First();
                    var firstF2 = loadF2.First();



                    //if (_ls.Count == 0)
                    //{
                    //    _ls = loadF1.ToList();
                    //}
                    var left1 = item.First().firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.First().firstField;
                    var right1 = item.Last().firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().firstField;
                    var left2 = item.First().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.First().sndField;
                    var right2 = item.Last().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().sndField;
                    var ff = from p in all_rec
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
                    //TODO: slow here
                    all_rec = new List<IDictionary<string, object>>(ff);// ff.ToDictionary(x=>x.Keys).ToList();
                    loadF1 = null;
                    loadF2 = null;
                    GC.Collect();
                }
                groupLinkageData = null;
            }
            else
            {
                // neu ko có linkage, check tất cả các Rule có phải viết cho 1 file ?
                // nếu có thì chọn xữ lý file đó
                var firstFileName_InRule = outputDataWithNameList.First().FileMapperName;
                //BL.WorkingSetItem onlyRuleForOneFile = null;
                if (outputDataWithNameList.All(p => p.FileMapperName == firstFileName_InRule))
                {
                    var onlyRuleForOneFile = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id && p.Filename == firstFileName_InRule);
                    if (onlyRuleForOneFile != null)
                    {
                        Console.WriteLine("Get data from file " + onlyRuleForOneFile.Filename);
                        loadF1 = Process_final(onlyRuleForOneFile.Id, limit: limit, addSequence: false, applyRules: false);
                        all_rec = loadF1.ToList();
                    }
                    
                }

                // new List<IDictionary<string, object>>(loadF1);
            }
            cached1 = null;
            cached2 = null;
            loadF1 = null;
            loadF2 = null;
            GC.Collect();
            // apply rule mapper
            
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
            var outputData_ = outputDataWithNameList.GroupBy(c => c.OutputFieldId).ToList();

            var rule_ = rules.ToList();
            foreach (var rule in rule_)
            {
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }
            

            //var dyna = new DynaExp();
            using (var dt = new System.Data.DataTable())
            {
                Console.WriteLine(DateTime.Now.ToShortTimeString());
                Console.WriteLine("Applying Rules...");
                foreach (var rec in all_rec)
                {
                    //var rec = rec;
                    foreach (var group_field in outputData_)
                    {

                        var ruleForThisField = rule_.Where(p => p.OutputFieldId == group_field.Key).ToList();
                        var fieldname = group_field.Key + EV.DOLLAR;
                        var mapper = group_field.First();
                        //TODO: nếu ko viết Rule, và chỉ có 1 field dc chọn để map
                        if (mapper.FieldMapperName != seq1Name && mapper.FieldMapperName != seq2Name)
                            if (ruleForThisField.Count == 0)
                            {
                                if (group_field.Count() == 1)
                                {
                                    if (!string.IsNullOrEmpty(mapper.FieldMapperName))
                                    {
                                        var _name = mapper.FileMapperName + ":" + mapper.FieldMapperName;
                                        _name = _name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
                                        rec.Add(mapper.FieldName, rec[_name]);
                                    }else
                                    {
                                        //var _name = mapper.FileMapperName + ":" + mapper.FieldMapperName;
                                        //_name = _name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
                                        rec.Add(mapper.FieldName, null);
                                    }
                                    
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
                                    rec.Add(rule_fullname, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));
                                }
                                else if (rule.Type == 2)//bool
                                {
                                    rec.Add(rule_fullname, dyna.IS(rule.ExpValue.FormatWith(rec)));
                                }
                                else if (rule.Type == 1)//string
                                {
                                    rec.Add(rule_fullname, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));
                                }
                                else if (rule.Type == 3)//string
                                {
                                    rec.Add(rule_fullname, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));
                                }
                                else if (rule.Type == 4)//string
                                {
                                    rec.Add(rule_fullname, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
                                }
                                if (rule == ruleForThisField.Last())
                                {
                                    rec.Add(mapper.FieldName, rec[rule_fullname]);
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
                Console.WriteLine("Done apply Rules: "+DateTime.Now.ToShortTimeString());
                var primaryKey = string.Empty;
                if (linkageData != null)
                {
                    var firstLinkage = linkageData.First();
                    primaryKey = firstLinkage.firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + firstLinkage.firstField;
                }
                else
                {
                    primaryKey = firstFileId.Filename.Replace(".", EV.DOT) + EV.DOLLAR + firstFileId.PrimaryKey;
                }


                //var group1 = _ls.ToList().GroupBy(p => p[primaryKey]);
                GC.Collect();
                //add sequence
                Console.WriteLine("Grouping and adding sequence");
                foreach (var _group in all_rec.GroupBy(p => p[primaryKey]))
                {
                    var increasement = 1;
                    foreach (var record in _group)
                    {
                        record.Add(seq1Name, 1);
                        record.Add(seq2Name, increasement);
                        increasement++;
                    }

                }
                GC.Collect();
                Console.WriteLine("Transforming to dt");
                dtAll = Ulti.ToDataTable(all_rec);
                //remove columns
                Console.WriteLine("Cleaning columns");
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
                    list_col_to_remove.Clear();
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
                        var cell = row[col];
                        var content = cell.ToString();
                        if (formatCell.Type == EV.TYPE_NUM)
                        {
                            try
                            {
                                if (!string.IsNullOrEmpty(content))
                                    cell = Math.Round(Convert.ToDecimal(cell), formatCell.Decimal);
                            }
                            catch (Exception ex)
                            {

                                throw new Exception("Binding driver field FAIL:column=" + col + ", value=" + content + Environment.NewLine
                                    + "Decimal=" + formatCell.Decimal + Environment.NewLine
                                    + Newtonsoft.Json.JsonConvert.SerializeObject(row) + Environment.NewLine
                                    + ex.Message + Environment.NewLine
                                    + ex.StackTrace

                                    );
                            }
                        }
                        else
                        {
                            
                            
                            if (!string.IsNullOrEmpty(content) && content.Length >= formatCell.Length)
                            {
                                cell = content.Substring(0, formatCell.Length);
                            }
                        }
                        formatCell = null;
                    }
                }
                Console.WriteLine("Writing file");
                var fileOutput = db.outputMappers.Find(ws.SeletedOutputId);
                var name = DateTime.Now.ToString("yyyyMMdd") + "_" + fileOutput.Name + "_" + ws.User + ".csv";
                Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
                    ws.State + "\\" + ws.County + "\\" + name, dtAll);
                all_rec.Clear();
                dtAll.Clear();
                dtAll.Dispose();
                GC.Collect();
                Console.WriteLine("-----------------------------");
                return name;
            }
        }
        private static IEnumerable<IDictionary<string, object>> Process_final(int fileid, decimal limit = 100, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
        {
            //int limit = 100;
            //const string tab = "\t";
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
                    FileName = wsFile.Filename
                }).ToList();

                var fields_sort = sortAndActions.OrderBy(x => x.Order)
                    .ToDictionary(x => x.FileName.Replace(".", EV.DOT) + EV.DOLLAR + x.FieldName, x => new SortField
                    {
                        name = x.FieldName,
                        duplicateAction = (DuplicateAction)x.DuplicatedAction,
                        sortType = (SortType)x.OrderType,
                        duplicateActionType = x.DuplicatedActionType
                    }

                    );
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


                var allrecs = new List<IDictionary<string, object>>();

                var sortActions = fields_sort.OrderBy(p => p.Value.duplicateAction);
                var hasKeepAllRows = sortActions.Count(p => p.Value.duplicateAction == DuplicateAction.KeepAllRows) > 0;
                
                foreach (var _group in file1.GroupBy(p => p[primaryKey]))
                {
                    //declare
                    var breakOtherRecords = false;
                    var ignoreAll = false;
                    var record = _group.FirstOrDefault();
                    var isResponseWithError = false;

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
                                //var v = _group.FirstOrDefault()[sortField.Key];
                                foreach (var rec in _group)
                                {
                                    rec[sortField.Key] = _group.FirstOrDefault()[sortField.Key];

                                }

                            }
                            else if (action == DuplicateAction.PickupLastValue)
                            {
                                if (!hasKeepAllRows) { breakOtherRecords = true; }
                                //var v = _group.LastOrDefault()[sortField.Key];
                                foreach (var rec in _group)
                                {
                                    rec[sortField.Key] = _group.LastOrDefault()[sortField.Key];

                                }

                            }
                            else if (action == DuplicateAction.PickupFirstUn_NULL_value)
                            {
                                if (!hasKeepAllRows) breakOtherRecords = true;
                                //var v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                                foreach (var rec in _group)
                                {
                                    rec[sortField.Key] = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];

                                }
                            }
                            else if (action == DuplicateAction.PickupMaximumValue)
                            {
                                if (!hasKeepAllRows) breakOtherRecords = true;
                                //var v = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                                foreach (var rec in _group)
                                {
                                    rec[sortField.Key] = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));

                                }
                            }
                            else if (action == DuplicateAction.PickupMinimumValue)
                            {
                                if (!hasKeepAllRows) breakOtherRecords = true;
                                //var v = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                                foreach (var rec in _group)
                                {
                                    rec[sortField.Key] = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));

                                }
                            }
                            else if (action == DuplicateAction.SumAllRow)
                            {
                                if (!hasKeepAllRows) breakOtherRecords = true;
                                //var v = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                                foreach (var rec in _group)
                                {
                                    rec[sortField.Key] = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));

                                }
                            }
                            //TODO: ConcatenateWithDelimiter phải xác định delimeter
                            else if (action == DuplicateAction.ConcatenateWithDelimiter)
                            {
                                if (!hasKeepAllRows) breakOtherRecords = true;

                                //var v = string.Join(",", _group.Select(i => i[sortField.Key]));
                                foreach (var rec in _group)
                                {
                                    rec[sortField.Key] = string.Join(",", _group.Select(i => i[sortField.Key]));

                                }
                            }
                            else if (action == DuplicateAction.KeepAllRows)
                            {
                                breakOtherRecords = false;

                            }
                        }
                        catch (Exception ex)
                        {

                            throw new Exception("ProcessFinal_FAIL at: " + sortField.Key
                                + ", sortType: " + action + Environment.NewLine + "Record: " + Environment.NewLine +
                                Newtonsoft.Json.JsonConvert.SerializeObject(_group, Newtonsoft.Json.Formatting.Indented)+ Environment.NewLine +
                                ex.Message+" "+ex.StackTrace
                                );
                        }
                    }
                    foreach (var rec in _group)
                    {

                        if (ignoreAll)
                            break;

                        //rec.Add(wsFile.Filename + EV.DOLLAR + "isDuplicated", 0);// = 0;
                        //var numOfPrimaryKeyFound = _group.Count();
                        //rec.Add(wsFile.Filename + EV.DOLLAR + "numOfPrimaryKeyFound", numOfPrimaryKeyFound);
                        //if (numOfPrimaryKeyFound > 1)
                        //{
                        //    if (isResponseWithError)
                        //        throw new Exception("ResponseWithError");
                        //    rec[wsFile.Filename + EV.DOLLAR + "isDuplicated"] = 1;

                        //}

                        allrecs.Add(rec);

                        if (breakOtherRecords)
                        {

                            break;
                        }

                    }

                }
                file1.Clear();
                file1 = null;
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

                
                //add sequence
                //if (addSequence)
                //    foreach (var _group in sorted_file1.GroupBy(p => p[primaryKey]))
                //    {
                //        var increasement = 1;
                //        foreach (var record in _group)
                //        {
                //            record.Add("seq", 1);
                //            record.Add("seq2", increasement);
                //            increasement++;
                //        }

                //    }
                //GC.Collect();

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
                    rules = null;

                }
                
                GC.Collect();
                db.Dispose();
                return sorted_file1;
            }
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

            using (var dt = new System.Data.DataTable())
            {
                foreach (var rule in rules)
                {
                    if (rule.Type == 0)//math
                    {
                        foreach (var rec in sorted_file1)
                        {
                            //var myUnderlyingObject = rec;
                            //var rule_result = rule.ExpValue.FormatWith(rec);
                            //TODO: dòng này xữ lý chậm
                            rec.Add(rule.Name, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));


                        }
                    }
                    else if (rule.Type == 2)//bool
                    {
                        foreach (var rec in sorted_file1)
                        {
                            //var myUnderlyingObject = rec;
                            //var rule_result = dyna.IS(rule.ExpValue.FormatWith(rec));
                            //TODO: dòng này xữ lý chậm
                            rec.Add(rule.Name, dyna.IS(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 1)//string
                    {
                        foreach (var rec in sorted_file1)
                        {
                            //var myUnderlyingObject = rec;
                            //var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
                            //TODO: dòng này xữ lý chậm
                            rec.Add(rule.Name, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 3)//FUNC_Num
                    {
                        foreach (var rec in sorted_file1)
                        {
                            //var myUnderlyingObject = rec;
                            //var rule_result = dyna.FUNC_Num(rule.ExpValue.FormatWith(rec));
                            //TODO: dòng này xữ lý chậm
                            rec.Add(rule.Name, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 4)//obj AS_IS/IF
                    {
                        foreach (var rec in sorted_file1)
                        {
                            //var myUnderlyingObject = rec;
                            //var rule_result = dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec));
                            //TODO: dòng này xữ lý chậm
                            rec.Add(rule.Name, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));


                        }
                    }

                }
            }
        }
    }
}
