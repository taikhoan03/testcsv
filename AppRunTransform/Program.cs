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
        private const int x_record_limited_proccess_apply_rules_GC = 4 * 1000 * 1000;
        static void Main(string[] args)
        {
            Console.Title = "RunTransform";
            //var a = "D:\\FA_in_out\\InputFile\\IX\\HARI\\EXEMPTION.csv";
            //var b = Ulti.readFromPath_AsDictionary2("EXEMPTION.csv", a, 200000000);
            //Console.WriteLine("done");
            //Process_final2(132, 2000000000);
            //var dic = new string[2000000][];
            //for (int i = 0; i < 2000000; i++)
            //{
            //    dic[i] = new string[50];
            //    for (int j = 0; j < 50; j++)
            //    {
            //        //rec.Add(j.ToString());
            //        dic[i][j] = j.ToString();
            //    }
            //    //dic.Add(rec);
            //}
            //Console.Write("done");
            //Console.ReadLine();
            Console.WriteLine("App is running...");
            //Console.WriteLine("PRess Enter");
            //Console.ReadLine();
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
                    Console.WriteLine("processing ws: " + req.WorkingSetId + ", at: " + DateTime.Now.ToShortTimeString());
                    req.Status = 1;
                    req.IsReady = true;
                    db.SaveChanges();
                    try
                    {
                        var oldOutputName = req.OutputName;
                        var watch = Stopwatch.StartNew();
                        var outputname=runProcess(req.WorkingSetId, true);
                        //remove old file
                        if (outputname != oldOutputName)
                        {
                            var ws = db.workingSets.Find(req.WorkingSetId);
                            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                            Config.Data.GetKey("output_folder_process"),
                                            ws.State,
                                            ws.County
                                            );

                            path = path + @"\" + oldOutputName;
                            if(File.Exists(path) && !string.IsNullOrEmpty(oldOutputName))
                                System.IO.File.Delete(path);
                        }
                        watch.Stop();
                        req.TimeCost = Convert.ToInt32(watch.Elapsed.TotalSeconds);
                        req.Status = 2;
                        req.Detail = "";
                        req.OutputName = outputname;
                        
                        
                        //
                        db.SaveChanges();
                        //Console.ReadLine();
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
        public static string runProcess(int id, bool cleanUpResult = true)
        {


            var db = new BL.DA_Model();
            var lookupTable = db.FACodeTables.ToList();
            var lookupItems = db.FACodes.ToList();
            var lookupRule = new BL.LookupRule(lookupTable, lookupItems);
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id);
            //if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            var limit = 2 * 1000 * 1000 * 1000;


            //all recs
            var all_rec = new List<Dictionary<string, object>>();// Enumerable.Empty<Dictionary<string, object>>();


            var dtAll = new DataTable();
            var numOfRun = 0;
            var cached1 = Enumerable.Empty<Dictionary<string, object>>();
            var cached2 = Enumerable.Empty<Dictionary<string, object>>();
            var loadF1 = Enumerable.Empty<Dictionary<string, object>>();
            var loadF2 = Enumerable.Empty<Dictionary<string, object>>();
            //declare RuleMapper 
            var fileOutput = db.outputMappers.Find(ws.SeletedOutputId);
            var outputFields = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            var outputData = db.outputDatas.Where(p => outputFields.Any(c => c.Id == p.OutputFieldId) && 
            p.WorkingSetId == ws.Id);
            //var outputDataWithName = from pp in outputFields
            //                         join p in outputData
            //                         on pp.Id equals p.OutputFieldId
            //                         into ps
            //                         from p_ in ps.DefaultIfEmpty()
            //                         select new outputDataWithName
            //                         {
            //                             FieldMapperName = p_==null?null:p_.FieldMapperName,
            //                             FileMapperName = p_ == null ? null : p_.FileMapperName,
            //                             Id = p_ == null ? 0 : p_.Id,
            //                             Order = p_ == null ? 0 : p_.Order,
            //                             OutputFieldId =p_ == null ? 0 : p_.OutputFieldId,
            //                             WorkingSetId = p_ == null ? 0 : p_.WorkingSetId,
            //                             FieldName = pp.Name
            //                         };

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
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId && p.WorkingSetId == id).ToList();//.OrderBy(p => p.Order);
            var seq1Name = "seq1";
            var seq2Name = "seq2";
            var outputDataWithNameList = outputDataWithName.ToList();
            WorkingSetItem onlyRuleForOneFile = null;
            ////END declare RuleMapper 
            //nạp dữ liệu vào all_rec
            Watch.watch_start();
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
                        loadF1 = Process_final3(item.First().firstId, limit: limit, addSequence: false, applyRules: true);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final3(item.First().sndId, limit: limit, addSequence: false, applyRules: true);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                        cached1 = loadF1;
                        cached2 = loadF2;
                        all_rec = loadF1.ToList();
                    }
                    else
                    {
                        loadF1 = cached2;
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final3(item.First().sndId, limit: limit, addSequence: false, applyRules: true);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                    }
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
                    //var defaultDic = new Dictionary<string, object>();
                    //var randKey = Guid.NewGuid().ToString();
                    //defaultDic.Add(randKey, "");
                    //var ff = from p in all_rec
                    //         join pp in loadF2
                    //         on p[left1].ToString() + p[right1].ToString()
                    //         equals pp[left2].ToString() + pp[right2].ToString()
                    //         into ps
                    //         from g in ps.DefaultIfEmpty()
                    //         //select p.Concat(g).ToDictionary(x => x.Key, x => x.Value);
                    //        select p.Concat(g == null ? new Dictionary<string, object>() : g).ToDictionary(x => x.Key, x => x.Value);

                    //create empty field on l2
                    //var firstRecF1 = loadF1.First();
                    //var firstRecF2 = loadF2.First();
                    //var addonField = firstRecF2.Where(p => !firstRecF1.ContainsKey(p.Key)).ToList();
                    //foreach (var itemleft in loadF1)
                    //{
                    //    foreach (var key_ in addonField)
                    //    {
                    //        itemleft.Add(key_.Key, "");
                    //    }
                    //}
                    var ff = from p in all_rec
                             join pp in loadF2
                             on p[left1].ToString() + p[right1].ToString()
                             equals pp[left2].ToString() + pp[right2].ToString()
                             into ps
                             from g in ps.DefaultIfEmpty()
                                 //select p.Concat(g).ToDictionary(x => x.Key, x => x.Value);
                                 select p.Concat(g == null ? new Dictionary<string, object>() : g).ToDictionary(x => x.Key, x => x.Value);
                             //select p.Concat(g).ToDictionary(x => x.Key, x => x.Value);

                    //var ff = from p in all_rec
                    //         join pp in loadF2.Data
                    //         on new
                    //         {
                    //             a = p[left1Index],
                    //             b = p[right1Index]
                    //         }
                    //         equals new
                    //         {
                    //             a = pp[left2Index],
                    //             b = pp[right2Index]
                    //         }
                    //         into ps
                    //         from g in ps//.DefaultIfEmpty()
                    //         select p.Concat(g).ToList();//.Concat(g).ToList();
                    //select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g).ToDictionary(x => x.Key, x => x.Value);
                    //TODO: slow here
                    all_rec = ff.ToList();// new List<IDictionary<string, object>>(ff);// ff.ToDictionary(x=>x.Keys).ToList();
                    //foreach (var _item in ff)
                    //{
                    //    all_rec.Add(new Dictionary<string, object>(_item));
                    //}
                    loadF1.Clear_Disposed();// = null;
                    loadF2.Clear_Disposed();// = null;
                }
                groupLinkageData = null;
            }
            else
            {
                // neu ko có linkage, check tất cả các Rule có phải viết cho 1 file ?
                // nếu có thì chọn xữ lý file đó
                var firstFileName_TransferMapping = outputDataWithNameList.First().FileMapperName;
                //BL.WorkingSetItem onlyRuleForOneFile = null;
                if (outputDataWithNameList.All(p => p.FileMapperName == firstFileName_TransferMapping))
                {
                    onlyRuleForOneFile = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id && p.Filename == firstFileName_TransferMapping);
                    if (onlyRuleForOneFile != null)
                    {
                        Console.WriteLine("Get data from file " + onlyRuleForOneFile.Filename);
                        loadF1 = Process_final3(onlyRuleForOneFile.Id, limit: limit, addSequence: false, applyRules: true);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                        //all_rec = loadF1.ToList();
                        foreach (var item in loadF1)
                        {
                            all_rec.Add(new Dictionary<string, object>(item));
                        }
                    }

                }

                // new List<IDictionary<string, object>>(loadF1);
            }

            cached1.Clear_Disposed();// = null;
            cached2.Clear_Disposed();// = null;
            loadF1.Clear_Disposed();// = null;
            loadF2.Clear_Disposed();// = null;
                                    // apply rule mapper

            //format _ normalizi
            var maxField = 0;
            var recordMaxFieldIndex = 0;
            for (int i = 0; i < all_rec.Count; i++)
            {
                if (all_rec[i].Count > maxField)
                {
                    maxField = all_rec[i].Count;
                    recordMaxFieldIndex = i;
                }
            }

            var recChuan = all_rec[recordMaxFieldIndex];
            for (int i = 0; i < all_rec.Count; i++)
            {
                var rec = all_rec[i];
                foreach (var item in recChuan)
                {
                    if (!rec.ContainsKey(item.Key))
                    {
                        rec.Add(item.Key, "");
                    }
                }
            }

            //outputDataWithNameList.Add(new outputDataWithName
            //{
            //    FieldMapperName = seq1Name,
            //    FileMapperName = seq1Name,
            //    Id = 0,
            //    Order = 999,
            //    OutputFieldId = -1,
            //    WorkingSetId = ws.Id,
            //    FieldName = seq1Name
            //});
            //outputDataWithNameList.Add(new outputDataWithName
            //{
            //    FieldMapperName = seq2Name,
            //    FileMapperName = seq2Name,
            //    Id = 0,
            //    Order = 999,
            //    OutputFieldId = -2,
            //    WorkingSetId = ws.Id,
            //    FieldName = seq2Name
            //});
            var outputData_ = outputDataWithNameList.GroupBy(c => c.OutputFieldId).ToList();

            var rule_ = rules.ToList();
            //rename field in rule expression
            foreach (var rule in rule_)
            {
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }


            var primaryKey = string.Empty;
            if (linkageData != null)
            {
                var firstLinkage = linkageData.First();
                primaryKey = firstLinkage.firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + firstLinkage.firstField;
            }
            else
            {
                if (onlyRuleForOneFile != null)
                {
                    primaryKey = onlyRuleForOneFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR + onlyRuleForOneFile.PrimaryKey;
                }
                else
                    primaryKey = firstFileId.Filename.Replace(".", EV.DOT) + EV.DOLLAR + firstFileId.PrimaryKey;
            }
            var firstRec_original_without_key = all_rec.First().Select(p => p.Key).Where(p => p != primaryKey).ToArray();

            var numOfGroupItems = outputData_.Count;
            var ls_outputFieldName = new string[numOfGroupItems];// List<string>();
            var ls_mappers = new outputDataWithName[numOfGroupItems];// List<outputDataWithName>();
            var ls_outputDataDetail = new Dictionary<string, List<OutputDataDetail>>();
            var ls_numOfFields = new int[numOfGroupItems];// List<int>();
            var ls_isSimpleInputType = new int[numOfGroupItems];// List<int>();
            var index = 0;
            foreach (var group_field in outputData_)
            {

                var rulesForThisField = rule_
                    .Where(p => p.OutputFieldId == group_field.Key).ToList();
                var fieldname = group_field.Key + EV.DOLLAR;
                var mapper = group_field.First();
                ls_outputFieldName[index] = fieldname;//.Add(fieldname);
                ls_outputDataDetail.Add(fieldname, rulesForThisField);
                ls_mappers[index] = mapper;//.Add(mapper);
                ls_numOfFields[index] = group_field.Count();//.Add(group_field.Count());

                var iIsSimpleInputType = 0;

                if (mapper.FieldMapperName != seq1Name && mapper.FieldMapperName != seq2Name)
                    if (rulesForThisField.Count == 0)
                    {
                        if (group_field.Count() == 1)
                        {
                            if (!string.IsNullOrEmpty(mapper.FieldMapperName))
                            {
                                iIsSimpleInputType = 2;
                            }
                            else
                            {
                                iIsSimpleInputType = 1;
                            }

                        }

                    }
                ls_isSimpleInputType[index] = iIsSimpleInputType;//.Add(iIsSimpleInputType);
                index++;
            }
            var icount = 0;
            //var dyna = new DynaExp();
            //transfer to editable
            var hsTmp_rule_to_remove = new List<string>();// ls_outputDataDetail.Select(p => p.Value.Select(c => c.));
            var hsTmp_rule_to_remove_length = 0;
            var hsTmp_rule_to_remove_need_to_add = true;

            //lookup
            using (var dt = new System.Data.DataTable())
            {

                Console.WriteLine("Applying Rules...");
                //TODO: nếu ko viết Rule, và chỉ có 1 field dc chọn để map
                var irec = 0;
                var linkRuleLookup = new Dictionary<object, object>();


                foreach (var rec in all_rec)
                {
                    for (int i = 0; i < ls_outputFieldName.Length; i++)
                    {
                        var fieldname = ls_outputFieldName[i];
                        var mapper = ls_mappers[i];
                        var rulesForThisField = ls_outputDataDetail[fieldname];
                        var numOfRules = rulesForThisField.Count;
                        var numOfFields = ls_numOfFields[i];
                        var inputType = ls_isSimpleInputType[i];

                        if (inputType == 2)
                        {
                            var _name = mapper.FileMapperName + ":" + mapper.FieldMapperName;
                            _name = _name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
                            rec.Add(mapper.FieldName, rec[_name]);
                        }
                        else if (inputType == 1)
                        {
                            rec.Add(mapper.FieldName, string.Empty);
                        }
                        else
                        {
                            //var lastRule = new OutputDataDetail();// rulesForThisField.Last();
                            //if (numOfRules > 0)
                            //{
                            //    lastRule = rulesForThisField[numOfRules - 1];
                            //}
                            //else
                            //{
                            //    lastRule = null;
                            //}
                            for (int j = 0; j < numOfRules; j++)
                            {
                                var rule = rulesForThisField[j];
                                var rule_fullname = fieldname + rule.Name;
                                if (hsTmp_rule_to_remove_need_to_add)
                                    hsTmp_rule_to_remove.Add(rule_fullname);
                                try
                                {

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
                                    else if (rule.Type == 6)//string
                                    {
                                        //rec.Add(rule_fullname, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
                                        var parts = rule.ExpValue.FormatWith(rec).Split(new string[] { "]]" }, StringSplitOptions.None);
                                        var tableName = parts[0];
                                        var val = parts[1];//parts[1]:Rule_8
                                        var rep = "NULL";
                                        if (!lookupRule.rules.ContainsKey(tableName)
                                            || !lookupRule.rules[tableName].ContainsKey(val))
                                            rep = "NULL";
                                        else
                                            rep = lookupRule.rules[tableName][val];
                                        rec.Add(rule_fullname, rep);
                                    }
                                    if (j == numOfRules - 1)
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
                    //flag
                    irec++;
                    if (irec == 1)
                    {
                        hsTmp_rule_to_remove_length = hsTmp_rule_to_remove.Count;
                        hsTmp_rule_to_remove_need_to_add = false;
                    }

                    //remove no need fields/old fields
                    for (int i = 0; i < hsTmp_rule_to_remove_length; i++)
                    {
                        rec.Remove(hsTmp_rule_to_remove[i]);
                    }
                    foreach (var item in firstRec_original_without_key)
                    {
                        rec.Remove(item);
                    }
                    //if (irec % 1000000 ==  999999)
                    //{
                    //    GC.Collect();
                    //    GC.WaitForPendingFinalizers();
                    //}
                }


               Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);

                //add sequence
                Console.WriteLine("Grouping and adding sequence");
                #region Sequence
                var firstRec = all_rec.First();
                var seqType = 0;
                var fileHasSeq1_only = new string[] { "Land", "Land Use", "Assessor Ownership", "Sales", "Situs Address", "Parcel to Parcel Cross Reference", "Assessor Land Values" };
                var fileHasSeq2 = new string[] { "Assessor Building Values", "Assessor Exemption Type", "Building" };
                var fileHasSeq3 = new string[] { "Building Permit", "Building Green Code", "Extra Feature", "Building Area" };
                var outputPrimaryKey = "UNFORMATTED_APN";

                if (!firstRec.ContainsKey(outputPrimaryKey))
                {
                    throw new Exception("<strong>Transform Mapping</strong> field <strong>" + outputPrimaryKey + "</strong> must be selected");
                }
                if (fileHasSeq1_only.Any(p => p == fileOutput.Name))
                {
                    foreach (var item in all_rec)
                    {
                        item["APN_SEQUENCE_NUMBER"] = 1;
                    }
                }
                #region SEQ2
                else if (fileHasSeq2.Any(p => p == fileOutput.Name))
                {
                    #region Building
                    if (fileOutput.Name == "Building")
                    {
                        var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                        var hasBuildingSeqField = all_rec.First().ContainsKey(sndFieldKey);
                        var mustHaveField = new string[] {
                            "BG_YEAR_BUILT",
                            "BG_ACTUAL_YEAR_BUILT",

                            "BG_REMODEL_MAJOR_YEAR_BUILT",
                            "BG_REMODEL_PARTIAL_YEAR_BUILT",
                            "BG_TOTAL_NBR_OF_ROOMS",
                            "BG_TOTAL_NBR_OF_BEDROOMS",
                            "BG_TOTAL_NBR_OF_BATHROOMS",
                            "BG_NBR_OF_PARTIAL_BATHS",
                            "BG_FULL_BATHS",
                            "BG_HALF_BATHS",
                            "BG_1_QTR_BATHS",
                            "BG_3_QTR_BATHS",
                            "BG_BATH_FIXTURES",
                            "BG_COUNTY_BATH_IMPROVEMENT_CODE\\DESC",
                            "BG_NBR_OF_PLUMBING_FIXTURES",
                            "BG_NBR_OF_DINING_ROOMS",
                            "BG_NBR_OF_FAMILY_ROOMS",
                            "BG_NBR_OF_LIVING_ROOMS",
                            "BG_NBR_OF_OTHER_ROOMS",
                            "BG_COUNTY_OTHER_ROOMS_DESCRIPTION_CODE\\DESC",
                            "BG_COUNTY_STORIES_NBR",
                            "BG_COUNTY_STORIES_CODE\\DESC",
                            "BG_STORY_HEIGHT",
                            "BG_COUNTY_CONSTRUCTION_TYPE_CODE\\DESC",
                            "BG_COUNTY_BUILDING_FIRE_INS_CLASS_CODE\\DESC",
                            "BG_COUNTY_BUILDING_CURRENT_CONDITION_CODE\\DESC",
                            "BG_COUNTY_BUILDING_QUALITY_CODE\\DESC",
                            "BG_COUNTY_BUILDING_TYPE_CODE\\DESC",
                            "BG_COUNTY_BUILDING_IMPROVE_CODE\\DESC",
                            "BG_COUNTY_STYLE_CODE\\DESC",
                            "BG_COUNTY_AIR_CONDITIONING_CODE\\DESC",
                            "BG_COUNTY_ATTIC_FINISH_CODE\\DESC",
                            "BG_COUNTY_BASEMENT_FINISH_CODE\\DESC",
                            "BG_COUNTY_ELECTRIC\\ENERGY_CODE\\DESC",
                            "BG_COUNTY_ELEVATOR_CODE\\DESC",
                            "BG_COUNTY_EXTERIOR_WALLS_CODE\\DESC",
                            "BG_FIREPLACE_NUMBER",
                            "BG_COUNTY_FIREPLACE_TYPE_CODE\\DESC",
                            "BG_COUNTY_FLOOR_CONSTRUCTION_CODE\\DESC",
                            "BG_COUNTY_FLOOR_COVERING_CODE\\DESC",
                            "BG_COUNTY_FOUNDATION_CODE\\DESC",
                            "BG_COUNTY_HEATING_CODE\\DESC",
                            "BG_COUNTY_HEATING_FUEL_TYPE_CODE\\DESC",
                            "BG_COUNTY_INTERIOR_WALLS_CODE\\DESC",
                            "BG_COUNTY_ROOF_COVER_CODE\\DESC",
                            "BG_COUNTY_ROOF_FRAME_CODE\\DESC",
                            "BG_COUNTY_ROOF_SHAPE_CODE\\DESC",
                            "BG_COUNTY_WATER_HEATER_CODE\\DESC",
                            "BG_COUNTY_WATER_HEATER_CODE\\DESC",
                            "BG_PATIO_SQUARE_FOOTAGE",
                            "BG_COUNTY_POOL_CODE\\DESC",
                            "BG_POOL_SQUARE_FOOTAGE",
                            "BG_COUNTY_PORCH_CODE\\DESC",
                            "BG_PORCH_SQUARE_FOOTAGE",
                            "BG_MANUFACTURED_HOME_LENGTH",
                            "BG_MANUFACTURED_HOME_WIDTH",
                            "BG_UNIVERSAL_BUILDING_SQ_FEET",
                            "BG_COUNTY_BUILDING_SQ_FEET_IND_CODE\\DESC",
                            "BG_SUM_OF_BUILDING_SQ_FEET",
                            "BG_SUM_OF_LIVING_SQ_FEET",
                            "BG_SUM_OF_GROUND_FLOOR_SQ_FEET",
                            "BG_SUM_OF_GROSS_SQ_FEET",
                            "BG_SUM_OF_ADJUSTED_GROSS_SQ_FEET",
                            "BG_SUM_OF_BASEMENT_SQ_FEET",
                            "BG_SUM_OF_ATTIC_SQ_FEET",
                            "BG_SUM_OF_GARAGE/PARKING_SQ_FT",
                            "BG_SUM_OF_ABOVE_GRADE_LIVING_SQ_FEET",
                            "BG_ADDITIONS_SQ_FEET",
                            "BG_LEED_CERTIFIED_YEAR",
                            "BG_COUNTY_LEED_CERTIFIED_SCORE_CODE\\DESC",
                            "BG_NAAB/NGBS_YEAR",
                            "BG_COUNTY_NAAB\\NGBS_SCORE_CODE\\DESC",
                            "BG_HERS_YEAR",
                            "BG_HERS_RATING",
                            "BG_ENERGY_STAR_QUALIFIED_YEAR",
                            "BG_OTHER_GREEN_CERTIFIED"

                        };
                        var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        //var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));


                        all_rec = CSVTransform.addSeq2(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);



                    }
                    #endregion Building
                    #region Assessor Building Values
                    else if (fileOutput.Name == "Assessor Building Values")
                    {

                        var sndFieldKey = "AVB_BUILDING_SEQ";
                        var hasBuildingSeqField = firstRec.ContainsKey(sndFieldKey);
                        var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var mustHaveField = new string[] {
                            "AVB_ASSD_IMPROVEMENT_VALUE",
                            "AVB_MKT_IMPROVEMENT_VALUE",
                            "AVB_APPR_IMPROVEMENT_VALUE",
                            "AVB_TAXABLE_IMPROVEMENT_VALUE" };
                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));
                        all_rec = CSVTransform.addSeq2(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);
                    }
                    #endregion Assessor Building Values
                    #region Assessor Exemption Type
                    else if (fileOutput.Name == "Assessor Exemption Type")
                    {
                        var sndFieldKey = "AVE_EXEMPTION_SEQUENCE_NUMBER";
                        var hasBuildingSeqField = all_rec.First().ContainsKey(sndFieldKey);
                        var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var mustHaveField = new string[] {
                            "AVE_COUNTY_EXEMPTION_CODE",
                            "AVE_COUNTY_EXEMPTION_DESCRIPTION",
                            "AVE_EXEMPTION_AMOUNT",
                            "AVE_EXEMPTION_PERCENTAGE" };
                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));
                        all_rec = CSVTransform.addSeq2(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);

                    }
                    #endregion Assessor Exemption Type



                }
                #endregion SEQ2
                #region SEQ3
                else if (fileHasSeq3.Any(p => p == fileOutput.Name))
                {
                    var message = "Adding Sequence: <strong>Transform Mapping</strong> should have <strong style='color:red'>{0}</strong> field in <strong>" + fileOutput.Name + "</strong> output file selected...";
                    #region Building Permit
                    if (fileOutput.Name == "Building Permit")
                    {
                        var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                        var thrFieldKey = "BGP_BUILDING_PERMIT_SEQUENCE_NUMBER";
                        var hasBuildingSeqField = firstRec.ContainsKey(sndFieldKey);
                        //gb=group building
                        var mustHaveField = new string[] {
                            "BGP_BUILDING_PERMIT_NBR",
                            "BGP_BUILDING_PERMIT_REASON",

                            "BGP_BUILDING_PERMIT_DATE",
                            "BGP_BUILDING_PERMIT_ESTIMATED_AMT",
                            "BGP_BUILDING_PERMIT_STATUS",
                            "BGP_BUILDING_PERMIT_PERCENT_COMPLETE",
                        };
                        var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var hasThrField = firstRec.ContainsKey(thrFieldKey);
                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));

                        all_rec = CSVTransform.addSeq3(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
                    }
                    #endregion Building Permit
                    #region Building Green Code
                    else if (fileOutput.Name == "Building Green Code")
                    {
                        var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                        var thrFieldKey = "BGG_BUILDING_GREEN_SEQUENCE_NUMBER";
                        var hasBuildingSeqField = firstRec.ContainsKey(sndFieldKey);
                        //gb=group building
                        var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var mustHaveField = new string[] {
                            "BGG_COUNTY_BUILDING_GREEN_CODE\\DESC"
                        };

                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));
                        var hasThrField = firstRec.ContainsKey(thrFieldKey);

                        all_rec = CSVTransform.addSeq3(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
                    }
                    #endregion Building Green Code
                    #region Extra Feature
                    else if (fileOutput.Name == "Extra Feature")
                    {
                        var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                        var thrFieldKey = "FEATURE_ID/SEQ";
                        var hasBuildingSeqField = firstRec.ContainsKey(sndFieldKey);
                        var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        //gb=group building
                        var mustHaveField = new string[] {
                            "EX_COUNTY_FEATURE_TYPE_ID",
                            "EX_COUNTY_FEATURE_RAW/DESC",
                            "EX_ASSESSED_FEATURE_VALUE",

                            "EX_LENGTH",
                            "EX_WIDTH",
                            "EX_HEIGHT",
                            "EX_MEASURE_UNITS",
                            "EX_FEATURE_YEAR_BUILT",
                        };


                        var hasThrField = firstRec.ContainsKey(thrFieldKey);
                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));


                        all_rec = CSVTransform.addSeq3(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
                        //foreach (var gb in all_rec.GroupBy(p => p[outputPrimaryKey].ToString()))
                        //{
                        //    var seq2 = 1;
                        //    var seq3 = 1;
                        //    var dic = new Dictionary<string, string>();
                        //    foreach (var record in gb)
                        //    {
                        //        var str = "";
                        //        var builder = new StringBuilder();
                        //        builder.Append(str);
                        //        foreach (var item in mustHaveFieldFiltered)
                        //        {
                        //            builder.Append(record[item]);
                        //        }
                        //        str = builder.ToString();

                        //        if (!dic.ContainsKey(str))
                        //        {
                        //            dic.Add(str, null);
                        //            record[sndFieldKey] = seq2;
                        //            tmp_all_rec.Add(record);


                        //            if (hasThrField)
                        //            {
                        //                if (!hasThrMapped)
                        //                {
                        //                    if (!hasSndMapped && !hasThrMapped)
                        //                    {
                        //                        record[thrFieldKey] = seq2;
                        //                    }
                        //                    else
                        //                    {
                        //                        record[thrFieldKey] = seq3;
                        //                    }
                        //                }

                        //            }
                        //            seq2++;
                        //            seq3++;
                        //        }
                        //    }


                        //}

                    }
                    #endregion Extra Feature
                    #region Building Area
                    else if (fileOutput.Name == "Building Area")
                    {
                        var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                        var thrFieldKey = "BGA_BUILDING_AREA_SEQUENCE_NUMBER";
                        var hasBuildingSeqField = firstRec.ContainsKey(sndFieldKey);
                        var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                        //gb=group building
                        var mustHaveField = new string[] {
                            "BGA_COUNTY_BUILDING_AREA_CODE\\DESC",
                            "BGA_BUILDING_AREA"
                        };


                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));
                        var hasThrField = firstRec.ContainsKey(thrFieldKey);

                        all_rec = CSVTransform.addSeq3(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);


                    }
                    #endregion Building Area

                }
                #endregion SEQ3


                #endregion
                Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                Console.WriteLine("Cleaning fields");
                //dtAll = Ulti.ToDataTable(all_rec);
                //remove columns

                if (cleanUpResult)
                {
                    var list_col_to_remove = new List<string>();
                    foreach (var col in firstRec)
                    {
                        if (!outputData_.Any(c => c.First().FieldName == col.Key))
                        {
                            list_col_to_remove.Add(col.Key);
                        }
                    }
                    var addEmptyMissingFields = outputFields.ToList().Where(p => !firstRec.Any(c => c.Key == p.Name)).ToList();
                    foreach (var record in all_rec)
                    {
                        foreach (var col in list_col_to_remove)
                        {
                            record.Remove(col);
                        }
                        //add missing fields
                        foreach (var f in addEmptyMissingFields)
                        {
                            record.Add(f.Name, "");
                        }
                        record.Remove(primaryKey);
                    }
                    list_col_to_remove.Clear();



                }

                //format, and length
                Console.WriteLine("Cleaning result");



                var colFields = firstRec.Select(p => p.Key).ToArray();// new List<string>();
                //foreach (DataColumn item in dtAll.Columns)
                //{
                //    if (item.ColumnName != seq1Name && item.ColumnName != seq2Name)
                //        colFields.Add(item.ColumnName);
                //}
                var outputDic = outputFields.Where(c => colFields.Any(d => d == c.Name)).ToDictionary(x => x.Name, x => x);
                colFields = outputDic.Select(p => p.Key).ToArray();
                var numOfField_ = colFields.Count();
                foreach (var record in all_rec)
                {
                    for (int i = 0; i < numOfField_; i++)
                    {
                        var fieldname = colFields[i];
                        var fieldInfo = outputDic[fieldname];
                        var content = record[fieldname].ToString();
                        if (fieldInfo.Type == EV.TYPE_NUM)
                        {
                            try
                            {
                                //if(record[fieldname].ToString()== "47.15576")
                                //{
                                //    var a = 1;
                                //}
                                if (!string.IsNullOrEmpty(content))
                                    record[fieldname] = Math.Round(Convert.ToDecimal(content),
                                        fieldInfo.Decimal);//.ToString("F"+ fieldInfo.Decimal);
                                
                            }
                            catch (Exception ex)
                            {

                                throw new Exception("Binding driver field FAIL:column=" + fieldname + ", value=" + content + Environment.NewLine
                                    + "Decimal=" + fieldInfo.Decimal + Environment.NewLine

                                    //+ Newtonsoft.Json.JsonConvert.SerializeObject(row) + Environment.NewLine
                                    + ex.Message + Environment.NewLine
                                    + ex.StackTrace

                                    );
                            }
                        }
                        else
                        {


                            if (!string.IsNullOrEmpty(content) && content.Length >= fieldInfo.Length)
                            {
                                record[fieldname] = content.Substring(0, fieldInfo.Length);
                            }
                        }
                    }

                }

                //foreach (DataRow row in dtAll.Rows)
                //{
                //    foreach (var colName in colFields)
                //    {
                //        var fieldInfo = outputDic[colName];
                //        var cell = row[colName];
                //        var content = cell.ToString();
                //        if (fieldInfo.Type == EV.TYPE_NUM)
                //        {
                //            try
                //            {
                //                if (!string.IsNullOrEmpty(content))
                //                    cell = Math.Round(Convert.ToDecimal(cell), fieldInfo.Decimal);
                //            }
                //            catch (Exception ex)
                //            {

                //                throw new Exception("Binding driver field FAIL:column=" + colName + ", value=" + content + Environment.NewLine
                //                    + "Decimal=" + fieldInfo.Decimal + Environment.NewLine

                //                    //+ Newtonsoft.Json.JsonConvert.SerializeObject(row) + Environment.NewLine
                //                    + ex.Message + Environment.NewLine
                //                    + ex.StackTrace

                //                    );
                //            }
                //        }
                //        else
                //        {


                //            if (!string.IsNullOrEmpty(content) && content.Length >= fieldInfo.Length)
                //            {
                //                cell = content.Substring(0, fieldInfo.Length);
                //            }
                //        }
                //    }
                //}
                Console.WriteLine("Writing file");
                //var reOrderRs=new List<>
                var name = DateTime.Now.ToString("yyyyMMdd") + "_" + fileOutput.Name + "_" + ws.User + ".txt";
                //ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
                //    ws.State + "\\" + ws.County + "\\" + name, all_rec);
                ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
                    ws.State + "\\" + ws.County + "\\" + name, all_rec,outputFields.Select(p=>p.Name).ToArray());
                all_rec.Clear();
                all_rec = null;
                dtAll.Clear();
                dtAll.Dispose();
                GC.Collect();

                Console.WriteLine("Time: " + Watch.watch_Elapsed().TotalSeconds);
                Watch.watchStop();
                Console.WriteLine("Done at: " + DateTime.Now.ToShortTimeString());

                Console.WriteLine("-----------------------------");
                Console.WriteLine("");
                return name;
            }
        }
        public static string genDecimalFormat(int numOfDecimal)
        {


//            decimalVar.ToString("#.##"); // returns "" when decimalVar == 0

//            or

//decimalVar.ToString("0.##"); // returns "0"  when decimalVar == 0


            var str = "";
            for (int i = 0; i < numOfDecimal; i++)
            {
                str += "#";
            }
            return "0." + str;
        }
        public static string runProcess2(int id, bool cleanUpResult = false)
        {


            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id);
            //if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            decimal limit = 50000000000;// 50 * 1000 * 1000 * 1000;


            //all recs
            var all_rec = new List<List<string>>(); //new List<Dictionary<string, object>>();// Enumerable.Empty<Dictionary<string, object>>();


            var numOfRun = 0;
            var cache= new BL._CSV();
            var cached1 = new BL._CSV();// new List<List<string>>();// Enumerable.Empty<Dictionary<string, object>>();
            var cached2 = new BL._CSV();// new List<List<string>>(); ;// Enumerable.Empty<Dictionary<string, object>>();
            var loadF1 = new BL._CSV();//new List<List<string>>(); //Enumerable.Empty<Dictionary<string, object>>();
            var loadF2 = new BL._CSV();//new List<List<string>>(); //Enumerable.Empty<Dictionary<string, object>>();
            //declare RuleMapper 
            var fileOutput = db.outputMappers.Find(ws.SeletedOutputId);
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
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId && p.WorkingSetId == id).ToList();//.OrderBy(p => p.Order);
            var seq1Name = "seq1";
            var seq2Name = "seq2";
            var outputDataWithNameList = outputDataWithName.ToList();
            WorkingSetItem onlyRuleForOneFile = null;
            ////END declare RuleMapper 
            //nạp dữ liệu vào all_rec
            Watch.watch_start();
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
                        loadF1 = Process_final2(item.First().firstId, limit: limit, addSequence: false, applyRules: true);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final2(item.First().sndId, limit: limit, addSequence: false, applyRules: true);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                        cached1 = loadF1;
                        cached2 = loadF2;
                        all_rec = loadF1.Data;// loadF1.ToList();
                    }
                    else
                    {
                        loadF1 = cached2;
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final2(item.First().sndId, limit: limit, addSequence: false, applyRules: true);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                    }
                    numOfRun++;
                    var firstF1 = loadF1.Data.First();
                    var firstF2 = loadF2.Data.First();



                    //if (_ls.Count == 0)
                    //{
                    //    _ls = loadF1.ToList();
                    //}
                    var left1 = item.First().firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.First().firstField;
                    var left1Index = loadF1.Name_index[left1];

                    var right1 = item.Last().firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().firstField;
                    var right1Index = loadF1.Name_index[right1];

                    var left2 = item.First().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.First().sndField;
                    var left2Index = loadF2.Name_index[left2];

                    var right2 = item.Last().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().sndField;
                    var right2Index = loadF2.Name_index[right2];
                    //foreach (var f1i in loadF1.da)
                    //{

                    //}

                    var ff = from p in all_rec
                             join pp in loadF2.Data
                             on new
                             {
                                 a = p[left1Index],
                                 b = p[right1Index]
                             }
                             equals new
                             {
                                 a = pp[left2Index],
                                 b = pp[right2Index]
                             }
                             into ps
                             from g in ps//.DefaultIfEmpty()
                             select p.Concat(g).ToList();//.Concat(g).ToList();
                    //select p.Concat(g == null ? new Dictionary<string, object>() : g).ToDictionary(x => x.Key, x => x.Value);
                    //select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g).ToDictionary(x => x.Key, x => x.Value);
                    //TODO: slow here
                    all_rec = ff.ToList();// new List<IDictionary<string, object>>(ff);// ff.ToDictionary(x=>x.Keys).ToList();
                    var i = 0;
                    cached2.Name_index = cached1.Name_index.Union(cached2.Name_index).ToDictionary(x=>x.Key,x=>i++);
                    cache.Data = all_rec;
                    cache.Name_index = cached2.Name_index;
                }
                groupLinkageData = null;
            }
            else
            {
                // neu ko có linkage, check tất cả các Rule có phải viết cho 1 file ?
                // nếu có thì chọn xữ lý file đó
                var firstFileName_TransferMapping = outputDataWithNameList.First().FileMapperName;
                //BL.WorkingSetItem onlyRuleForOneFile = null;
                if (outputDataWithNameList.All(p => p.FileMapperName == firstFileName_TransferMapping))
                {
                    onlyRuleForOneFile = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id && p.Filename == firstFileName_TransferMapping);
                    if (onlyRuleForOneFile != null)
                    {
                        Console.WriteLine("Get data from file " + onlyRuleForOneFile.Filename);
                        cache = Process_final2(onlyRuleForOneFile.Id, limit: limit, addSequence: false, applyRules: false);
                        Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
                        all_rec = cache.Data;
                        
                    }

                }

            }


            var outputData_ = outputDataWithNameList.GroupBy(c => c.OutputFieldId).ToList();

            var rule_ = rules.ToList();
            //rename field in rule expression
            foreach (var rule in rule_)
            {
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }


            var primaryKey = string.Empty;
            if (linkageData != null)
            {
                var firstLinkage = linkageData.First();
                primaryKey = firstLinkage.firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + firstLinkage.firstField;
            }
            else
            {
                if (onlyRuleForOneFile != null)
                {
                    primaryKey = onlyRuleForOneFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR + onlyRuleForOneFile.PrimaryKey;
                }
                else
                    primaryKey = firstFileId.Filename.Replace(".", EV.DOT) + EV.DOLLAR + firstFileId.PrimaryKey;
            }
            var fields_original_without_key = cache.Name_index.Select(p => p.Key).Where(p => p != primaryKey).ToArray();
            //var fields_original_without_key = cache.Name_index.Select(p => p.Key).Where(p => p != primaryKey).ToArray();

            var numOfGroupItems = outputData_.Count;
            var ls_outputFieldName = new string[numOfGroupItems];// List<string>();
            var ls_mappers = new outputDataWithName[numOfGroupItems];// List<outputDataWithName>();
            var ls_outputDataDetail = new Dictionary<string, List<OutputDataDetail>>();
            var ls_numOfFields = new int[numOfGroupItems];// List<int>();
            var ls_isSimpleInputType = new int[numOfGroupItems];// List<int>();
            var ls_numOfRuleOnFieldIndex = new int[numOfGroupItems];
            var index = 0;
            
            foreach (var group_field in outputData_)
            {

                var rulesForThisField = rule_
                    .Where(p => p.OutputFieldId == group_field.Key).ToList();
                var fieldname = group_field.Key + EV.DOLLAR;
                var mapper = group_field.First();
                ls_outputFieldName[index] = fieldname;//.Add(fieldname);
                ls_outputDataDetail.Add(fieldname, rulesForThisField);
                ls_numOfRuleOnFieldIndex[index] = rulesForThisField.Count;
                ls_mappers[index] = mapper;//.Add(mapper);
                ls_numOfFields[index] = group_field.Count();//.Add(group_field.Count());

                var iIsSimpleInputType = 0;

                if (mapper.FieldMapperName != seq1Name && mapper.FieldMapperName != seq2Name)
                    if (rulesForThisField.Count == 0)
                    {
                        if (group_field.Count() == 1)
                        {
                            if (!string.IsNullOrEmpty(mapper.FieldMapperName))
                            {
                                iIsSimpleInputType = 2;
                            }
                            else
                            {
                                iIsSimpleInputType = 1;
                            }

                        }

                    }
                ls_isSimpleInputType[index] = iIsSimpleInputType;//.Add(iIsSimpleInputType);
                index++;
            }
            //add rule columns -> empty
            var numOfAddonFields = 0;
            for (int i = 0; i < ls_mappers.Length; i++)
            {
                var c_rules = ls_outputDataDetail.Skip(i).Take(1).First();
                if (c_rules.Value.Count > 0)
                {
                    foreach (var rule in c_rules.Value)
                    {
                        //var _name = mapper.FileMapperName + ":" + mapper.FieldMapperName;
                        //_name = _name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);

                        cache.Name_index.Add(c_rules.Key + rule.Name, cache.Name_index.Max(c => c.Value) + 1);
                        numOfAddonFields++;
                    }
                    cache.Name_index.Add(ls_mappers[i].FieldName, cache.Name_index.Max(c => c.Value) + 1);
                    numOfAddonFields++;
                }
                else
                {
                    cache.Name_index.Add(ls_mappers[i].FieldName, cache.Name_index.Max(c => c.Value) + 1);
                    numOfAddonFields++;
                }
                
            }
            foreach (var item in all_rec)
            {
                for (int i = 0; i < numOfAddonFields; i++)
                {
                    item.Add("");
                }
            }

            var cc = ls_outputDataDetail.SelectMany(p=>p.Value);
            var icount = 0;
            //var dyna = new DynaExp();
            //transfer to editable
            var hsTmp_rule_to_remove = new List<string>();// ls_outputDataDetail.Select(p => p.Value.Select(c => c.));
            var hsTmp_rule_to_remove_length = 0;
            var hsTmp_rule_to_remove_need_to_add = true;
            Console.WriteLine("Applying Rules...");
            var dt = new System.Data.DataTable();
            var colToRemove = new List<string>(fields_original_without_key);
            var is_added_new_header = false;
            var tmp_new_header = new List<string>();
            for (int irec = 0; irec < all_rec.Count; irec++)
            {
                
                var rec = all_rec[irec];
                var dicrec = new Dictionary<string, object>();
                var icol = 0;
                foreach (var col in cache.Name_index)
                {
                    dicrec.Add(col.Key, rec[icol]);
                    icol++;
                }
                if (irec == 1)
                {
                    hsTmp_rule_to_remove_length = hsTmp_rule_to_remove.Count;
                    hsTmp_rule_to_remove_need_to_add = false;
                    
                }

                for (int i = 0; i < ls_outputFieldName.Length; i++)
                {
                    var fieldname = ls_outputFieldName[i];
                    var mapper = ls_mappers[i];
                    var rulesForThisField = ls_outputDataDetail[fieldname];
                    var numOfRules = ls_numOfRuleOnFieldIndex[i];// rulesForThisField.Count;
                    var numOfFields = ls_numOfFields[i];
                    var inputType = ls_isSimpleInputType[i];

                    if (inputType == 2)
                    {
                        var _name = mapper.FileMapperName + ":" + mapper.FieldMapperName;
                        _name = _name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
                        dicrec[mapper.FieldName] = dicrec[_name];
                        //rec.Add(mapper.FieldName, rec[_name]);
                    }
                    else if (inputType == 1)
                    {
                        //rec.Add(mapper.FieldName, string.Empty);
                    }
                    else
                    {
                        
                        for (int j = 0; j < numOfRules; j++)
                        {
                            var rule = rulesForThisField[j];
                            var rule_fullname = fieldname + rule.Name;
                            if (hsTmp_rule_to_remove_need_to_add)
                            {
                                hsTmp_rule_to_remove.Add(rule_fullname);
                                colToRemove.Add(rule_fullname);//.Union(hsTmp_rule_to_remove);
                            }
                                
                            try
                            {

                                if (rule.Type == 0)
                                {
                                    //var rule_result = rule.ExpValue.FormatWith(rec);
                                    //TODO: dòng này xữ lý chậm
                                    dicrec[rule_fullname] = dt.Compute(rule.ExpValue.FormatWith(dicrec), "");// target.Eval(rule_result));
                                }
                                else if (rule.Type == 2)//bool
                                {
                                    dicrec[rule_fullname] = dyna.IS(rule.ExpValue.FormatWith(dicrec));
                                }
                                else if (rule.Type == 1)//string
                                {
                                    dicrec[rule_fullname] = dyna.FORMAT(rule.ExpValue.FormatWith(dicrec));
                                }
                                else if (rule.Type == 3)//string
                                {
                                    dicrec[rule_fullname] = dyna.FUNC_Num(rule.ExpValue.FormatWith(dicrec));
                                }
                                else if (rule.Type == 4)//string
                                {
                                    dicrec[rule_fullname] = dyna.FUNC_Obj(rule.ExpValue.FormatWith(dicrec));
                                }
                                if (j == numOfRules - 1)
                                {
                                    dicrec[mapper.FieldName] = dicrec[rule_fullname];
                                    //dicrec.Add(mapper.FieldName, dicrec[rule_fullname]);

                                }
                            }
                            catch (Exception ex)
                            {

                                throw new Exception("Fail to run Rule:" + rule.ExpValue + Environment.NewLine +
                                    " rec: " + Newtonsoft.Json.JsonConvert.SerializeObject(dicrec) + Environment.NewLine +
                                    " Message:" + ex.Message
                                    );
                            }
                        }


                    }


                }
                //flag
                //irec++;
                
                foreach (var item in colToRemove)
                {
                    dicrec.Remove(item);
                }
                if (!is_added_new_header)
                {
                    is_added_new_header = true;
                    tmp_new_header = dicrec.Select(p => p.Key).ToList();
                    
                }
                all_rec[irec]= dicrec.Select(p => p.Value.ToString()).ToList();
                //dicrec.Clear();
                if (irec % x_record_limited_proccess_apply_rules_GC == x_record_limited_proccess_apply_rules_GC-1)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }
            var ii = 0;
            cache.Name_index = tmp_new_header.ToDictionary(x => x, x => ii++);
            dt.Clear();
            dt.Dispose();
            dt = null;
            GC.Collect();
            Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);



            //add sequence
            Console.WriteLine("Grouping and adding sequence");
            #region Sequence
            var columns_after_apply_rules = cache.Name_index.Select(p=>p.Key).ToList();// all_rec.First();
            var seqType = 0;
            var fileHasSeq1_only = new string[] { "Land", "Land Use", "Assessor Ownership", "Sales", "Situs Address", "Parcel to Parcel Cross Reference", "Assessor Land Values" };
            var fileHasSeq2 = new string[] { "Assessor Building Values", "Assessor Exemption Type", "Building" };
            var fileHasSeq3 = new string[] { "Building Permit", "Building Green Code", "Extra Feature", "Building Area" };
            var outputPrimaryKey = "UNFORMATTED_APN";
            var primarykeyIndex = cache.getColumnIndex(outputPrimaryKey);

            if (!columns_after_apply_rules.Any(p=>p== outputPrimaryKey))
            {
                throw new Exception("<strong>Transform Mapping</strong> field <strong>" + outputPrimaryKey + "</strong> must be selected");
            }
            //try
            //{
            //    var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
            //    var thrFieldKey = "BGP_BUILDING_PERMIT_SEQUENCE_NUMBER";
            //    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
            //    //gb=group building
            //    var fields_condition_should_have = new string[] {
            //                "BGP_BUILDING_PERMIT_NBR",
            //                "BGP_BUILDING_PERMIT_REASON",

            //                "BGP_BUILDING_PERMIT_DATE",
            //                "BGP_BUILDING_PERMIT_ESTIMATED_AMT",
            //                "BGP_BUILDING_PERMIT_STATUS",
            //                "BGP_BUILDING_PERMIT_PERCENT_COMPLETE",
            //            };
            //    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
            //    var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
            //    var hasThrField = columns_after_apply_rules.Any(p => p == thrFieldKey);// firstRec.ContainsKey(thrFieldKey);
            //    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));

            //    addSeq3(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
            //}
            //catch (Exception)
            //{

            //    throw;
            //}
            if (fileHasSeq1_only.Any(p => p == fileOutput.Name))
            {
                foreach (var item in all_rec)
                {
                    //item["APN_SEQUENCE_NUMBER"] = 1;
                    item.Add("1");
                }
                cache.Name_index.Add("APN_SEQUENCE_NUMBER", cache.Name_index.Max(c => c.Value) + 1);
            }
            #region SEQ2
            else if (fileHasSeq2.Any(p => p == fileOutput.Name))
            {
                #region Building
                if (fileOutput.Name == "Building")
                {
                    var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
                    var fields_condition_should_have = new string[] {
                        "BG_YEAR_BUILT",
                        "BG_ACTUAL_YEAR_BUILT",

                        "BG_REMODEL_MAJOR_YEAR_BUILT",
                        "BG_REMODEL_PARTIAL_YEAR_BUILT",
                        "BG_TOTAL_NBR_OF_ROOMS",
                        "BG_TOTAL_NBR_OF_BEDROOMS",
                        "BG_TOTAL_NBR_OF_BATHROOMS",
                        "BG_NBR_OF_PARTIAL_BATHS",
                        "BG_FULL_BATHS",
                        "BG_HALF_BATHS",
                        "BG_1_QTR_BATHS",
                        "BG_3_QTR_BATHS",
                        "BG_BATH_FIXTURES",
                        "BG_COUNTY_BATH_IMPROVEMENT_CODE\\DESC",
                        "BG_NBR_OF_PLUMBING_FIXTURES",
                        "BG_NBR_OF_DINING_ROOMS",
                        "BG_NBR_OF_FAMILY_ROOMS",
                        "BG_NBR_OF_LIVING_ROOMS",
                        "BG_NBR_OF_OTHER_ROOMS",
                        "BG_COUNTY_OTHER_ROOMS_DESCRIPTION_CODE\\DESC",
                        "BG_COUNTY_STORIES_NBR",
                        "BG_COUNTY_STORIES_CODE\\DESC",
                        "BG_STORY_HEIGHT",
                        "BG_COUNTY_CONSTRUCTION_TYPE_CODE\\DESC",
                        "BG_COUNTY_BUILDING_FIRE_INS_CLASS_CODE\\DESC",
                        "BG_COUNTY_BUILDING_CURRENT_CONDITION_CODE\\DESC",
                        "BG_COUNTY_BUILDING_QUALITY_CODE\\DESC",
                        "BG_COUNTY_BUILDING_TYPE_CODE\\DESC",
                        "BG_COUNTY_BUILDING_IMPROVE_CODE\\DESC",
                        "BG_COUNTY_STYLE_CODE\\DESC",
                        "BG_COUNTY_AIR_CONDITIONING_CODE\\DESC",
                        "BG_COUNTY_ATTIC_FINISH_CODE\\DESC",
                        "BG_COUNTY_BASEMENT_FINISH_CODE\\DESC",
                        "BG_COUNTY_ELECTRIC\\ENERGY_CODE\\DESC",
                        "BG_COUNTY_ELEVATOR_CODE\\DESC",
                        "BG_COUNTY_EXTERIOR_WALLS_CODE\\DESC",
                        "BG_FIREPLACE_NUMBER",
                        "BG_COUNTY_FIREPLACE_TYPE_CODE\\DESC",
                        "BG_COUNTY_FLOOR_CONSTRUCTION_CODE\\DESC",
                        "BG_COUNTY_FLOOR_COVERING_CODE\\DESC",
                        "BG_COUNTY_FOUNDATION_CODE\\DESC",
                        "BG_COUNTY_HEATING_CODE\\DESC",
                        "BG_COUNTY_HEATING_FUEL_TYPE_CODE\\DESC",
                        "BG_COUNTY_INTERIOR_WALLS_CODE\\DESC",
                        "BG_COUNTY_ROOF_COVER_CODE\\DESC",
                        "BG_COUNTY_ROOF_FRAME_CODE\\DESC",
                        "BG_COUNTY_ROOF_SHAPE_CODE\\DESC",
                        "BG_COUNTY_WATER_HEATER_CODE\\DESC",
                        "BG_COUNTY_WATER_HEATER_CODE\\DESC",
                        "BG_PATIO_SQUARE_FOOTAGE",
                        "BG_COUNTY_POOL_CODE\\DESC",
                        "BG_POOL_SQUARE_FOOTAGE",
                        "BG_COUNTY_PORCH_CODE\\DESC",
                        "BG_PORCH_SQUARE_FOOTAGE",
                        "BG_MANUFACTURED_HOME_LENGTH",
                        "BG_MANUFACTURED_HOME_WIDTH",
                        "BG_UNIVERSAL_BUILDING_SQ_FEET",
                        "BG_COUNTY_BUILDING_SQ_FEET_IND_CODE\\DESC",
                        "BG_SUM_OF_BUILDING_SQ_FEET",
                        "BG_SUM_OF_LIVING_SQ_FEET",
                        "BG_SUM_OF_GROUND_FLOOR_SQ_FEET",
                        "BG_SUM_OF_GROSS_SQ_FEET",
                        "BG_SUM_OF_ADJUSTED_GROSS_SQ_FEET",
                        "BG_SUM_OF_BASEMENT_SQ_FEET",
                        "BG_SUM_OF_ATTIC_SQ_FEET",
                        "BG_SUM_OF_GARAGE/PARKING_SQ_FT",
                        "BG_SUM_OF_ABOVE_GRADE_LIVING_SQ_FEET",
                        "BG_ADDITIONS_SQ_FEET",
                        "BG_LEED_CERTIFIED_YEAR",
                        "BG_COUNTY_LEED_CERTIFIED_SCORE_CODE\\DESC",
                        "BG_NAAB/NGBS_YEAR",
                        "BG_COUNTY_NAAB\\NGBS_SCORE_CODE\\DESC",
                        "BG_HERS_YEAR",
                        "BG_HERS_RATING",
                        "BG_ENERGY_STAR_QUALIFIED_YEAR",
                        "BG_OTHER_GREEN_CERTIFIED"

                    };
                    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    //var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    //todo: work here
                    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));

                    //cache.da
                    addSeq2(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);



                }
                #endregion Building
                #region Assessor Building Values
                else if (fileOutput.Name == "Assessor Building Values")
                {

                    var sndFieldKey = "AVB_BUILDING_SEQ";
                    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
                    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var fields_condition_should_have = new string[] {
                        "AVB_ASSD_IMPROVEMENT_VALUE",
                        "AVB_MKT_IMPROVEMENT_VALUE",
                        "AVB_APPR_IMPROVEMENT_VALUE",
                        "AVB_TAXABLE_IMPROVEMENT_VALUE" };
                    //var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));
                    //all_rec = addSeq2(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);
                    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));

                    //cache.da
                    addSeq2(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);
                }
                #endregion Assessor Building Values
                #region Assessor Exemption Type
                else if (fileOutput.Name == "Assessor Exemption Type")
                {
                    var sndFieldKey = "AVE_EXEMPTION_SEQUENCE_NUMBER";
                    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
                    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var fields_condition_should_have = new string[] {
                        "AVE_COUNTY_EXEMPTION_CODE",
                        "AVE_COUNTY_EXEMPTION_DESCRIPTION",
                        "AVE_EXEMPTION_AMOUNT",
                        "AVE_EXEMPTION_PERCENTAGE" };
                    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));
                    addSeq2(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);

                }
                #endregion Assessor Exemption Type



            }
            #endregion SEQ2
            #region SEQ3
            else if (fileHasSeq3.Any(p => p == fileOutput.Name))
            {
                var message = "Adding Sequence: <strong>Transform Mapping</strong> should have <strong style='color:red'>{0}</strong> field in <strong>" + fileOutput.Name + "</strong> output file selected...";
                #region Building Permit
                if (fileOutput.Name == "Building Permit")
                {
                    var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                    var thrFieldKey = "BGP_BUILDING_PERMIT_SEQUENCE_NUMBER";
                    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
                    //gb=group building
                    var fields_condition_should_have = new string[] {
                        "BGP_BUILDING_PERMIT_NBR",
                        "BGP_BUILDING_PERMIT_REASON",

                        "BGP_BUILDING_PERMIT_DATE",
                        "BGP_BUILDING_PERMIT_ESTIMATED_AMT",
                        "BGP_BUILDING_PERMIT_STATUS",
                        "BGP_BUILDING_PERMIT_PERCENT_COMPLETE",
                    };
                    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var hasThrField = columns_after_apply_rules.Any(p => p == thrFieldKey);// firstRec.ContainsKey(thrFieldKey);
                    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));

                    addSeq3(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
                }
                #endregion Building Permit
                #region Building Green Code
                else if (fileOutput.Name == "Building Green Code")
                {
                    var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                    var thrFieldKey = "BGG_BUILDING_GREEN_SEQUENCE_NUMBER";
                    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
                    //gb=group building
                    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var fields_condition_should_have = new string[] {
                        "BGG_COUNTY_BUILDING_GREEN_CODE\\DESC"
                    };

                    var hasThrField = columns_after_apply_rules.Any(p => p == thrFieldKey);// firstRec.ContainsKey(thrFieldKey);
                    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));

                    addSeq3(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
                }
                #endregion Building Green Code
                #region Extra Feature
                else if (fileOutput.Name == "Extra Feature")
                {
                    var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                    var thrFieldKey = "FEATURE_ID/SEQ";
                    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
                    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    //gb=group building
                    var fields_condition_should_have = new string[] {
                        "EX_COUNTY_FEATURE_TYPE_ID",
                        "EX_COUNTY_FEATURE_RAW/DESC",
                        "EX_ASSESSED_FEATURE_VALUE",

                        "EX_LENGTH",
                        "EX_WIDTH",
                        "EX_HEIGHT",
                        "EX_MEASURE_UNITS",
                        "EX_FEATURE_YEAR_BUILT",
                    };


                    var hasThrField = columns_after_apply_rules.Any(p => p == thrFieldKey);// firstRec.ContainsKey(thrFieldKey);
                    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));

                    addSeq3(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);


                }
                #endregion Extra Feature
                #region Building Area
                else if (fileOutput.Name == "Building Area")
                {
                    var sndFieldKey = "BUILDING_SEQUENCE_NUMBER";
                    var thrFieldKey = "BGA_BUILDING_AREA_SEQUENCE_NUMBER";
                    var hasBuildingSeqField = columns_after_apply_rules.Any(p => p == sndFieldKey);
                    var hasSndMapped = ls_mappers.Any(p => p.FieldName == sndFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    var hasThrMapped = ls_mappers.Any(p => p.FieldName == thrFieldKey && !string.IsNullOrEmpty(p.FieldMapperName));
                    //gb=group building
                    var fields_condition_should_have = new string[] {
                        "BGA_COUNTY_BUILDING_AREA_CODE\\DESC",
                        "BGA_BUILDING_AREA"
                    };


                    var hasThrField = columns_after_apply_rules.Any(p => p == thrFieldKey);// firstRec.ContainsKey(thrFieldKey);
                    var mustHaveFieldFiltered = fields_condition_should_have.Where(p => columns_after_apply_rules.Contains(p));

                    addSeq3(primarykeyIndex, cache, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);


                }
                #endregion Building Area

            }
            #endregion SEQ3
            #endregion sequence



            ////format, and length
            //Console.WriteLine("Cleaning result");



            //var colFields = firstRec.Select(p => p.Key).ToArray();// new List<string>();
            ////foreach (DataColumn item in dtAll.Columns)
            ////{
            ////    if (item.ColumnName != seq1Name && item.ColumnName != seq2Name)
            ////        colFields.Add(item.ColumnName);
            ////}
            var outputDic = outputFields.Where(c => columns_after_apply_rules.Any(d => d == c.Name)).ToArray();//.ToDictionary(x => x.Name, x => x);
            
            var numOfField_ = outputDic.Count();
            var outputColumnsIndexs = new int[numOfField_];
            var iTmp = 0;
            foreach (var item in outputDic)
            {
                outputColumnsIndexs[iTmp] = cache.getColumnIndex(item.Name);
                iTmp++;
            }
            primarykeyIndex = cache.getColumnIndex(primaryKey);
            foreach (var record in cache.Data)
            {
                
                for (int i = 0; i < numOfField_; i++)
                {
                    var colIndex = outputColumnsIndexs[i];
                    var fieldInfo = outputDic[i];
                    var fieldname = fieldInfo.Name;
                    
                    var content = cache.get(fieldname, record);// record[fieldname].ToString();
                    if (fieldInfo.Type == EV.TYPE_NUM)
                    {
                        try
                        {
                            if (!string.IsNullOrEmpty(content))
                                record[colIndex] = Math.Round(Convert.ToDecimal(content),
                                    fieldInfo.Decimal).ToString();
                        }
                        catch (Exception ex)
                        {

                            throw new Exception("Binding driver field FAIL:column=" + fieldname + ", value=" + content + Environment.NewLine
                                + "Decimal=" + fieldInfo.Decimal + Environment.NewLine

                                //+ Newtonsoft.Json.JsonConvert.SerializeObject(row) + Environment.NewLine
                                + ex.Message + Environment.NewLine
                                + ex.StackTrace

                                );
                        }
                    }
                    else
                    {


                        if (!string.IsNullOrEmpty(content) && content.Length >= fieldInfo.Length)
                        {
                            record[colIndex] = content.Substring(0, fieldInfo.Length);
                        }
                    }
                }
                record.RemoveAt(primarykeyIndex);
            }
            //re-header
            cache.Name_index = outputDic.ToDictionary(x => x.Name, x => 0);
            Console.WriteLine("Writing file");

            var name = DateTime.Now.ToString("yyyyMMdd") + "_" + fileOutput.Name + "_" + ws.User + ".csv";
            ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
                ws.State + "\\" + ws.County + "\\" + name, cache);
            all_rec.Clear();
            all_rec = null;
            GC.Collect();

            Console.WriteLine("Time: " + Watch.watch_Elapsed().TotalSeconds);
            Watch.watchStop();
            Console.WriteLine("Done at: " + DateTime.Now.ToShortTimeString());

            Console.WriteLine("-----------------------------");
            Console.WriteLine("");
            return name;
            //using (var dt2 = new System.Data.DataTable())
            //{

                

                


            //    //#endregion
            //    //Console.WriteLine("--- Time: " + Watch.watch_Elapsed().TotalSeconds);
            //    //Console.WriteLine("Cleaning fields");
            //    ////dtAll = Ulti.ToDataTable(all_rec);
            //    ////remove columns

            //    //if (cleanUpResult)
            //    //{
            //    //    var list_col_to_remove = new List<string>();
            //    //    foreach (var col in firstRec)
            //    //    {
            //    //        if (!outputData_.Any(c => c.First().FieldName == col.Key))
            //    //        {
            //    //            list_col_to_remove.Add(col.Key);
            //    //        }
            //    //    }
            //    //    foreach (var record in all_rec)
            //    //    {
            //    //        foreach (var col in list_col_to_remove)
            //    //        {
            //    //            record.Remove(col);
            //    //        }
            //    //    }
            //    //    list_col_to_remove.Clear();



            //    //}

            //    ////format, and length
            //    //Console.WriteLine("Cleaning result");



            //    //var colFields = firstRec.Select(p => p.Key).ToArray();// new List<string>();
            //    ////foreach (DataColumn item in dtAll.Columns)
            //    ////{
            //    ////    if (item.ColumnName != seq1Name && item.ColumnName != seq2Name)
            //    ////        colFields.Add(item.ColumnName);
            //    ////}
            //    //var outputDic = outputFields.Where(c => colFields.Any(d => d == c.Name)).ToDictionary(x => x.Name, x => x);
            //    //var numOfField_ = colFields.Count();
            //    //foreach (var record in all_rec)
            //    //{
            //    //    for (int i = 0; i < numOfField_; i++)
            //    //    {
            //    //        var fieldname = colFields[i];
            //    //        var fieldInfo = outputDic[fieldname];
            //    //        var content = record[fieldname].ToString();
            //    //        if (fieldInfo.Type == EV.TYPE_NUM)
            //    //        {
            //    //            try
            //    //            {
            //    //                if (!string.IsNullOrEmpty(content))
            //    //                    record[fieldname] = Math.Round(Convert.ToDecimal(content),
            //    //                        fieldInfo.Decimal);
            //    //            }
            //    //            catch (Exception ex)
            //    //            {

            //    //                throw new Exception("Binding driver field FAIL:column=" + fieldname + ", value=" + content + Environment.NewLine
            //    //                    + "Decimal=" + fieldInfo.Decimal + Environment.NewLine

            //    //                    //+ Newtonsoft.Json.JsonConvert.SerializeObject(row) + Environment.NewLine
            //    //                    + ex.Message + Environment.NewLine
            //    //                    + ex.StackTrace

            //    //                    );
            //    //            }
            //    //        }
            //    //        else
            //    //        {


            //    //            if (!string.IsNullOrEmpty(content) && content.Length >= fieldInfo.Length)
            //    //            {
            //    //                record[fieldname] = content.Substring(0, fieldInfo.Length);
            //    //            }
            //    //        }
            //    //    }

            //    //}

            //    //Console.WriteLine("Writing file");

            //    //var name = DateTime.Now.ToString("yyyyMMdd") + "_" + fileOutput.Name + "_" + ws.User + ".csv";
            //    //ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
            //    //    ws.State + "\\" + ws.County + "\\" + name, all_rec);
            //    //all_rec.Clear();
            //    //all_rec = null;
            //    //GC.Collect();

            //    //Console.WriteLine("Time: " + Watch.watch_Elapsed().TotalSeconds);
            //    //Watch.watchStop();
            //    //Console.WriteLine("Done at: " + DateTime.Now.ToShortTimeString());

            //    //Console.WriteLine("-----------------------------");
            //    //Console.WriteLine("");
            //    //return name;
            //}
        }
        private static IEnumerable<Dictionary<string, object>> Process_final3(int fileid, decimal limit = 2*1000*1000*1000, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
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
                var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                        Config.Data.GetKey("input_folder_process"),
                                        ws.State,
                                        ws.County
                                        );
                path = path + @"\" + wsFile.Filename;
                //var test= Helpers.ReadCSV.ReadAsArray(wsFile.Filename, path, limit);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    //test.get("EXEMPTION@csv$EXEMPTION_DSCR", test.Data[3000000]);
                //    test.get("EXEMPTION@csv$EXEMPTION_DSCR", 3000000);
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                var tab = KnownDelimeter(path);
                var file1 = ReadCSV.ReadAsDictionary("", path, limit, tab);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    var oo=file1[3000000]["EXEMPTION@csv$EXEMPTION_DSCR"];
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                var primaryKey = wsFile.PrimaryKey;// wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR + wsFile.PrimaryKey.ReplaceUnusedCharacters();
                if (string.IsNullOrEmpty(primaryKey))
                {
                    throw new Exception("No Primary Key, Please select 1 first");
                }


                var allrecs = new List<Dictionary<string, object>>();

                var sortActions = fields_sort.OrderBy(p => p.Value.duplicateAction);
                var hasKeepAllRows = sortActions.Count(p => p.Value.duplicateAction == DuplicateAction.KeepAllRows) > 0;

                foreach (var _group in file1.GroupBy(p => p[primaryKey]))
                {
                    //declare
                    var breakOtherRecords = false;
                    var ignoreAll = false;
                    //var record = _group.FirstOrDefault();
                    var isResponseWithError = false;
                    var r_last = _group.Last();
                    var r_first = _group.First();
                    foreach (var sortField in sortActions)
                    {
                        var action = sortField.Value.duplicateAction;

                        try
                        {
                            var delimiter = sortField.Value.delimiter;
                            var v = new object();
                            switch (action)
                            {
                                case DuplicateAction.ResponseWithError:
                                    isResponseWithError = true;
                                    break;
                                case DuplicateAction.PickupFirstValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = r_first[sortField.Key];
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;
                                    }
                                    break;
                                case DuplicateAction.PickupLastValue:
                                    if (!hasKeepAllRows) { breakOtherRecords = true; }
                                    v = r_last[sortField.Key];
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.PickupFirstUn_NULL_value:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    //p.ContainsKey(sortField.Key) && 
                                    v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    //if(v!=null)
                                    //    if (!string.IsNullOrEmpty(v.ToString()))
                                    //    {

                                    //    } 
                                    break;
                                case DuplicateAction.PickupMaximumValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.PickupMinimumValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.SumAllRow:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.ConcatenateWithDelimiter:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    var deli = "";
                                    if (!string.IsNullOrEmpty(delimiter))
                                        deli = delimiter;
                                    v = string.Join(deli, _group.Select(i => i[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;
                                    }
                                    break;
                                case DuplicateAction.KeepAllRows:
                                    breakOtherRecords = false;
                                    break;
                                default:
                                    break;
                            }


                            #region use-if
                            //if (action == DuplicateAction.ResponseWithError)
                            //{
                            //    //throw new Exception("ResponseWithError");
                            //    isResponseWithError = true;
                            //}
                            //else if (action == DuplicateAction.PickupFirstValue)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    var v = r_first[sortField.Key];
                            //    foreach (var rec in _group)
                            //    {
                            //        //rec[sortField.Key] = _group.FirstOrDefault()[sortField.Key];
                            //        rec[sortField.Key] = v;
                            //    }

                            //}
                            //else if (action == DuplicateAction.PickupLastValue)
                            //{
                            //    if (!hasKeepAllRows) { breakOtherRecords = true; }
                            //    var v = r_last[sortField.Key];
                            //    foreach (var rec in _group)
                            //    {
                            //        //rec[sortField.Key] = _group.LastOrDefault()[sortField.Key];
                            //        rec[sortField.Key] = v;

                            //    }

                            //}
                            //else if (action == DuplicateAction.PickupFirstUn_NULL_value)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    var v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            //    foreach (var rec in _group)
                            //    {
                            //        //rec[sortField.Key] = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            //        rec[sortField.Key] = v;

                            //    }
                            //}
                            //else if (action == DuplicateAction.PickupMaximumValue)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    var v = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = v;

                            //    }
                            //}
                            //else if (action == DuplicateAction.PickupMinimumValue)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    //var v = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            //    var _val = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = _val;

                            //    }
                            //}
                            //else if (action == DuplicateAction.SumAllRow)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    //var v = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                            //    var _val = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = _val;

                            //    }
                            //}
                            ////TODO: ConcatenateWithDelimiter phải xác định delimeter
                            //else if (action == DuplicateAction.ConcatenateWithDelimiter)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;

                            //    var v = string.Join(",", _group.Select(i => i[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = v;

                            //    }
                            //}
                            //else if (action == DuplicateAction.KeepAllRows)
                            //{
                            //    breakOtherRecords = false;

                            //}
                            #endregion

                        }
                        catch (Exception ex)
                        {

                            throw new Exception("ProcessFinal_Sorting_FAIL at: " + sortField.Key
                                + ", sortType: " + action + Environment.NewLine + "Record: " + Environment.NewLine +
                                Newtonsoft.Json.JsonConvert.SerializeObject(_group, Newtonsoft.Json.Formatting.Indented) + Environment.NewLine +
                                ex.Message + " " + ex.StackTrace
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

                        allrecs.Add(new Dictionary<string, object>(rec));

                        if (breakOtherRecords)
                        {

                            break;
                        }

                    }

                }
                file1.Clear_Disposed();
                file1.Clear();
                file1 = null;
                //Sorting


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
                    CallFunction(rules, sorted_file1, wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR, lookupRule);
                    rules = null;

                }
                //allrecs.Clear();
                //allrecs = null;

            }
            //GC.Collect();
            return sorted_file1;
        }
        private static void TranferColumnsToRecord()
        {

        }
        private static void addSeq2(int primaryKeyIndex,
            _CSV csv,
            string[] mustHaveFieldFiltered,
            string sndFieldKey,
            bool hasSndMapped,
            bool hasSndField
            )
        {
            //var tmp_all_rec = new List<Dictionary<string, object>>();
            var APN_SEQUENCE_NUMBER_index = csv.getColumnIndex("APN_SEQUENCE_NUMBER");
            var sndFieldKeyIndex= csv.getColumnIndex(sndFieldKey);
            if (hasSndField)
            {
                foreach (var gb in csv.Data.GroupBy(p => p[primaryKeyIndex].ToString()))
                {
                    var seq2 = 1;

                    var ls_check = new List<string>();
                    foreach (var record in gb)
                    {
                        record[APN_SEQUENCE_NUMBER_index] = "1";
                        var str = "";

                        foreach (var item in mustHaveFieldFiltered)
                        {
                            str += csv.get(item, record);
                        }
                        if (!ls_check.Any(p => p == str))
                        {
                            record[sndFieldKeyIndex] = seq2.ToString();
                            seq2++;
                        }
                    }


                }
            }else
            {
                var seq2name = "seq2";
                csv.Name_index.Add(seq2name, csv.Name_index.Max(p => p.Value));
                var newSeq2Index = csv.getColumnIndex(seq2name);
                foreach (var gb in csv.Data.GroupBy(p => p[primaryKeyIndex].ToString()))
                {
                    var seq2 = 1;

                    var ls_check = new List<string>();
                    foreach (var record in gb)
                    {
                        record[APN_SEQUENCE_NUMBER_index] = "1";
                        
                        var str = "";

                        foreach (var item in mustHaveFieldFiltered)
                        {
                            str += csv.get(item, record);
                        }
                        if (!ls_check.Any(p => p == str))
                        {
                            record.Add(seq2.ToString());
                            seq2++;
                        }
                    }


                }
                
            }
                
            //return tmp_all_rec;
        }
        private static List<Dictionary<string, object>> addSeq2(string outputPrimaryKey,
            List<Dictionary<string, object>> all_rec,
            string[] mustHaveFieldFiltered,
            string sndFieldKey,
            bool hasSndMapped,
            bool hasSndField
            )
        {
            var tmp_all_rec = new List<Dictionary<string, object>>();
            if (hasSndField)
                foreach (var gb in all_rec.GroupBy(p => p[outputPrimaryKey].ToString()))
                {
                    var seq2 = 1;
                    
                    var dic = new Dictionary<string, string>();
                    foreach (var record in gb)
                    {
                        record["APN_SEQUENCE_NUMBER"] = 1;
                        var str = "";
                        var builder = new StringBuilder();
                        builder.Append(str);
                        foreach (var item in mustHaveFieldFiltered)
                        {
                            builder.Append(record[item]);
                        }
                        str = builder.ToString();

                        if (!dic.ContainsKey(str))
                        {
                            dic.Add(str, null);
                            record[sndFieldKey] = seq2;
                            tmp_all_rec.Add(record);


                            
                            seq2++;
                        }
                    }


                }
            return tmp_all_rec;
        }
        private static void addSeq3(int outputPrimaryIndex,
            _CSV csv,
            string[] mustHaveFieldFiltered,
            string sndFieldKey,
            string thrFieldKey,
            bool hasSndMapped,
            bool hasThrMapped,
            bool hasSndField,
            bool hasThrField
            )
        {
            var tmp_all_rec = new List<Dictionary<string, object>>();
            var newSeq2Index = csv.getColumnIndex(sndFieldKey);
            var newSeq3Index = csv.getColumnIndex(thrFieldKey);
            var APN_SEQUENCE_NUMBER_index = csv.getColumnIndex("APN_SEQUENCE_NUMBER");
            if (hasSndField)
                foreach (var gb in csv.Data.GroupBy(p => p[outputPrimaryIndex].ToString()))
                {
                    var seq2 = 1;
                    var seq3 = 1;

                    var ls_check = new List<string>();
                    foreach (var record in gb)
                    {
                        record[APN_SEQUENCE_NUMBER_index] = "1";
                        if (!hasSndMapped)
                        {
                            var str = "";

                            foreach (var item in mustHaveFieldFiltered)
                            {
                                str += csv.get(item, record);
                            }
                            if (!ls_check.Any(p => p == str))
                            {
                                record[newSeq2Index] = seq2.ToString();
                                if (hasThrField)
                                {
                                    if (!hasThrMapped)
                                    {
                                        if (!hasSndMapped && !hasThrMapped)
                                        {
                                            record[newSeq3Index] = seq2.ToString();
                                        }
                                        else
                                        {
                                            record[newSeq3Index] = seq3.ToString();
                                        }
                                    }

                                }
                                seq2++;
                                seq3++;
                            }



                            //var str = "";
                            //var builder = new StringBuilder();
                            //builder.Append(str);
                            //foreach (var item in mustHaveFieldFiltered)
                            //{
                            //    builder.Append(record[item]);
                            //}
                            //str = builder.ToString();

                            //if (!dic.ContainsKey(str))
                            //{
                            //    dic.Add(str, null);
                            //    record[sndFieldKey] = seq2;
                            //    tmp_all_rec.Add(record);


                            //    if (hasThrField)
                            //    {
                            //        if (!hasThrMapped)
                            //        {
                            //            if (!hasSndMapped && !hasThrMapped)
                            //            {
                            //                record[thrFieldKey] = seq2;
                            //            }
                            //            else
                            //            {
                            //                record[thrFieldKey] = seq3;
                            //            }
                            //        }

                            //    }
                            //    seq2++;
                            //    seq3++;
                            //}
                        }
                        else
                        {
                            //record[thrFieldKey] = seq2;
                            if (hasThrField)
                            {
                                if (!hasThrMapped)
                                {
                                    if (!hasSndMapped && !hasThrMapped)
                                    {
                                        record[newSeq3Index] = seq2.ToString();
                                    }
                                    else
                                    {
                                        record[newSeq3Index] = seq3.ToString();
                                    }
                                }

                            }
                            seq2++;
                            seq3++;
                            //tmp_all_rec.Add(record);
                        }

                    }


                }
        }
        private static List<Dictionary<string, object>> addSeq3(string outputPrimaryKey,
            List<Dictionary<string, object>> all_rec,
            string[] mustHaveFieldFiltered,
            string sndFieldKey,
            string thrFieldKey,
            bool hasSndMapped,
            bool hasThrMapped,
            bool hasSndField,
            bool hasThrField
            )
        {
            var tmp_all_rec = new List<Dictionary<string, object>>();
            if(hasSndField)
            foreach (var gb in all_rec.GroupBy(p => p[outputPrimaryKey].ToString()))
            {
                var seq2 = 1;
                var seq3 = 1;
                var dic = new Dictionary<string, string>();
                foreach (var record in gb)
                {
                    record["APN_SEQUENCE_NUMBER"] = 1;
                        if (!hasSndMapped)
                        {
                            var str = "";
                            var builder = new StringBuilder();
                            builder.Append(str);
                            foreach (var item in mustHaveFieldFiltered)
                            {
                                builder.Append(record[item]);
                            }
                            str = builder.ToString();

                            if (!dic.ContainsKey(str))
                            {
                                dic.Add(str, null);
                                record[sndFieldKey] = seq2;
                                tmp_all_rec.Add(record);


                                if (hasThrField)
                                {
                                    if (!hasThrMapped)
                                    {
                                        if (!hasSndMapped && !hasThrMapped)
                                        {
                                            record[thrFieldKey] = seq2;
                                        }
                                        else
                                        {
                                            record[thrFieldKey] = seq3;
                                        }
                                    }

                                }
                                seq2++;
                                seq3++;
                            }
                        }else
                        {
                            //record[thrFieldKey] = seq2;
                            if (hasThrField)
                            {
                                if (!hasThrMapped)
                                {
                                    if (!hasSndMapped && !hasThrMapped)
                                    {
                                        record[thrFieldKey] = seq2;
                                    }
                                    else
                                    {
                                        record[thrFieldKey] = seq3;
                                    }
                                }

                            }
                            seq2++;
                            seq3++;
                            tmp_all_rec.Add(record);
                        }
                            
                }


            }
            return tmp_all_rec;
        }
        private static IEnumerable<Dictionary<string, object>> Process_final(int fileid, decimal limit = 100, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
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
                    Delimiter=p.ConcatenateWithDelimiter
                }).ToList();

                var fields_sort = sortAndActions.OrderBy(x => x.Order)
                    .ToDictionary(x => x.FileName.Replace(".", EV.DOT) + EV.DOLLAR + x.FieldName, x => new SortField
                    {
                        name = x.FieldName,
                        duplicateAction = (DuplicateAction)x.DuplicatedAction,
                        sortType = (SortType)x.OrderType,
                        duplicateActionType = x.DuplicatedActionType,
                        delimiter=x.Delimiter
                    }

                    );
                var fieldTypes = db.jobFileLayouts.Where(p => p.WorkingSetItemId == fileid).ToDictionary(p => p.Fieldname, p => p.Type);
                var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                        Config.Data.GetKey("input_folder_process"),
                                        ws.State,
                                        ws.County
                                        );
                path = path + @"\" + wsFile.Filename;
                //var test= Helpers.ReadCSV.ReadAsArray(wsFile.Filename, path, limit);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    //test.get("EXEMPTION@csv$EXEMPTION_DSCR", test.Data[3000000]);
                //    test.get("EXEMPTION@csv$EXEMPTION_DSCR", 3000000);
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                var tab = KnownDelimeter(path);
                var a = ReadCSV.ReadAsDictionary("", path, limit, "Reason", "ChangeDocId", tab);
                var file1 = ReadCSV.ReadAsDictionary(wsFile.Filename, path, limit,tab);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    var oo=file1[3000000]["EXEMPTION@csv$EXEMPTION_DSCR"];
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                var primaryKey = wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR + wsFile.PrimaryKey.ReplaceUnusedCharacters();
                if (string.IsNullOrEmpty(primaryKey))
                {
                    throw new Exception("No Primary Key, Please select 1 first");
                }


                var allrecs = new List<Dictionary<string, object>>();

                var sortActions = fields_sort.OrderBy(p => p.Value.duplicateAction);
                var hasKeepAllRows = sortActions.Count(p => p.Value.duplicateAction == DuplicateAction.KeepAllRows) > 0;

                foreach (var _group in file1.GroupBy(p => p[primaryKey]))
                {
                    //declare
                    var breakOtherRecords = false;
                    var ignoreAll = false;
                    //var record = _group.FirstOrDefault();
                    var isResponseWithError = false;
                    var r_last = _group.Last();
                    var r_first = _group.First();
                    foreach (var sortField in sortActions)
                    {
                        var action = sortField.Value.duplicateAction;

                        try
                        {
                            var delimiter = sortField.Value.delimiter;
                            var v = new object();
                            switch (action)
                            {
                                case DuplicateAction.ResponseWithError:
                                    isResponseWithError = true;
                                    break;
                                case DuplicateAction.PickupFirstValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = r_first[sortField.Key];
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;
                                    }
                                    break;
                                case DuplicateAction.PickupLastValue:
                                    if (!hasKeepAllRows) { breakOtherRecords = true; }
                                    v = r_last[sortField.Key];
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.PickupFirstUn_NULL_value:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.PickupMaximumValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.PickupMinimumValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.SumAllRow:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;

                                    }
                                    break;
                                case DuplicateAction.ConcatenateWithDelimiter:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    var deli = "";
                                    if (!string.IsNullOrEmpty(delimiter))
                                        deli = delimiter;
                                    v = string.Join(deli, _group.Select(i => i[sortField.Key]));
                                    foreach (var rec in _group)
                                    {
                                        rec[sortField.Key] = v;
                                    }
                                    break;
                                case DuplicateAction.KeepAllRows:
                                    breakOtherRecords = false;
                                    break;
                                default:
                                    break;
                            }


                            #region use-if
                            //if (action == DuplicateAction.ResponseWithError)
                            //{
                            //    //throw new Exception("ResponseWithError");
                            //    isResponseWithError = true;
                            //}
                            //else if (action == DuplicateAction.PickupFirstValue)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    var v = r_first[sortField.Key];
                            //    foreach (var rec in _group)
                            //    {
                            //        //rec[sortField.Key] = _group.FirstOrDefault()[sortField.Key];
                            //        rec[sortField.Key] = v;
                            //    }

                            //}
                            //else if (action == DuplicateAction.PickupLastValue)
                            //{
                            //    if (!hasKeepAllRows) { breakOtherRecords = true; }
                            //    var v = r_last[sortField.Key];
                            //    foreach (var rec in _group)
                            //    {
                            //        //rec[sortField.Key] = _group.LastOrDefault()[sortField.Key];
                            //        rec[sortField.Key] = v;

                            //    }

                            //}
                            //else if (action == DuplicateAction.PickupFirstUn_NULL_value)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    var v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            //    foreach (var rec in _group)
                            //    {
                            //        //rec[sortField.Key] = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            //        rec[sortField.Key] = v;

                            //    }
                            //}
                            //else if (action == DuplicateAction.PickupMaximumValue)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    var v = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = v;

                            //    }
                            //}
                            //else if (action == DuplicateAction.PickupMinimumValue)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    //var v = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            //    var _val = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = _val;

                            //    }
                            //}
                            //else if (action == DuplicateAction.SumAllRow)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;
                            //    //var v = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                            //    var _val = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = _val;

                            //    }
                            //}
                            ////TODO: ConcatenateWithDelimiter phải xác định delimeter
                            //else if (action == DuplicateAction.ConcatenateWithDelimiter)
                            //{
                            //    if (!hasKeepAllRows) breakOtherRecords = true;

                            //    var v = string.Join(",", _group.Select(i => i[sortField.Key]));
                            //    foreach (var rec in _group)
                            //    {
                            //        rec[sortField.Key] = v;

                            //    }
                            //}
                            //else if (action == DuplicateAction.KeepAllRows)
                            //{
                            //    breakOtherRecords = false;

                            //}
                            #endregion

                        }
                        catch (Exception ex)
                        {

                            throw new Exception("ProcessFinal_Sorting_FAIL at: " + sortField.Key
                                + ", sortType: " + action + Environment.NewLine + "Record: " + Environment.NewLine +
                                Newtonsoft.Json.JsonConvert.SerializeObject(_group, Newtonsoft.Json.Formatting.Indented) + Environment.NewLine +
                                ex.Message + " " + ex.StackTrace
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

                        allrecs.Add(new Dictionary<string, object>(rec));

                        if (breakOtherRecords)
                        {

                            break;
                        }

                    }

                }
                file1.Clear_Disposed();
                file1.Clear();
                file1 = null;
                //Sorting


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


                //apply rules
                if (applyRules)
                {
                    var rules = db.fieldRules.Where(p => p.WorkingSetItemId == fileid).OrderBy(p => p.Order).ToList();
                    //update rules as part of fieldType
                    foreach (var rule in rules)
                    {
                        fieldTypes.Add(rule.Name, rule.Type);
                    }
                    CallFunction(rules, sorted_file1);
                    rules = null;

                }
                //allrecs.Clear();
                //allrecs = null;

            }
            GC.Collect();
            return sorted_file1;
        }
        private static BL._CSV Process_final2(int fileid, decimal limit = 100, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
        {
            //int limit = 100;
            //const string tab = "\t";
            var sorted_file1 = Enumerable.Empty<List<string>>().OrderBy(x => 1);
            var file1 = new BL._CSV();
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
                //var test= Helpers.ReadCSV.ReadAsArray(wsFile.Filename, path, limit);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    //test.get("EXEMPTION@csv$EXEMPTION_DSCR", test.Data[3000000]);
                //    test.get("EXEMPTION@csv$EXEMPTION_DSCR", 3000000);
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                var tab = KnownDelimeter(path);
                file1 = ReadCSV.ReadAsArray(wsFile.Filename, path, limit, tab);
                //for (int i = 0; i < 20000000; i++)
                //{
                //    var oo=file1[3000000]["EXEMPTION@csv$EXEMPTION_DSCR"];
                //    //var l = new List<int> { 1, 2, 3, 4, 5 };
                //    //var ix = l.IndexOf(4);
                //}
                var primaryKey = wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR + wsFile.PrimaryKey.ReplaceUnusedCharacters();
                if (string.IsNullOrEmpty(primaryKey))
                {
                    throw new Exception("No Primary Key, Please select 1 first");
                }


                var allrecs = new List<List<string>>();

                var sortActions = fields_sort.OrderBy(p => p.Value.duplicateAction);
                var hasKeepAllRows = sortActions.Count(p => p.Value.duplicateAction == DuplicateAction.KeepAllRows) > 0;
                var priInd = file1.Name_index[primaryKey];

                var dicSortField = new Dictionary<string, int>(sortAndActions.Count);
                foreach (var item in sortActions)
                {
                    dicSortField.Add(item.Key, file1.Name_index[item.Key]);
                }
                var groupByKey = file1.Data.GroupBy(p => p[0]);
                foreach (var _group in groupByKey)
                {
                    //declare
                    var breakOtherRecords = false;
                    var ignoreAll = false;
                    //var record = _group.FirstOrDefault();
                    var isResponseWithError = false;
                    var r_last = _group.Last();
                    var r_first = _group.First();
                    foreach (var sortField in sortActions)
                    {
                        var action = sortField.Value.duplicateAction;
                        var ind = dicSortField[sortField.Key];
                        try
                        {
                            var v = "";
                            switch (action)
                            {
                                case DuplicateAction.ResponseWithError:
                                    isResponseWithError = true;
                                    break;
                                case DuplicateAction.PickupFirstValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = r_first[ind];
                                    foreach (var rec in _group)
                                    {
                                        rec[ind] = v;
                                    }
                                    break;
                                case DuplicateAction.PickupLastValue:
                                    if (!hasKeepAllRows) { breakOtherRecords = true; }
                                    v = r_last[ind];
                                    foreach (var rec in _group)
                                    {
                                        rec[ind] = v;

                                    }
                                    break;
                                case DuplicateAction.PickupFirstUn_NULL_value:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[ind].ToString()))[ind];
                                    foreach (var rec in _group)
                                    {
                                        rec[ind] = v;

                                    }
                                    break;
                                case DuplicateAction.PickupMaximumValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    decimal c = _group.Max(p => Convert.ToDecimal(p[ind]));
                                    foreach (var rec in _group)
                                    {
                                        rec[ind] = c.ToString();

                                    }
                                    break;
                                case DuplicateAction.PickupMinimumValue:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    decimal c2 = _group.Min(p => Convert.ToDecimal(p[ind]));
                                    foreach (var rec in _group)
                                    {
                                        rec[ind] = c2.ToString();

                                    }
                                    break;
                                case DuplicateAction.SumAllRow:
                                    if (!hasKeepAllRows) breakOtherRecords = true;
                                    decimal c3 = _group.Sum(p => Convert.ToDecimal(p[ind]));
                                    foreach (var rec in _group)
                                    {
                                        rec[ind] = c3.ToString();

                                    }
                                    break;
                                case DuplicateAction.ConcatenateWithDelimiter:
                                    if (!hasKeepAllRows) breakOtherRecords = true;

                                    v = string.Join(",", _group.Select(i => i[ind]));
                                    foreach (var rec in _group)
                                    {
                                        rec[ind] = v;
                                    }
                                    break;
                                case DuplicateAction.KeepAllRows:
                                    breakOtherRecords = false;
                                    break;
                                default:
                                    break;
                            }



                        }
                        catch (Exception ex)
                        {

                            throw new Exception("ProcessFinal_Sorting_FAIL at: " + sortField.Key
                                + ", sortType: " + action + Environment.NewLine + "Record: " + Environment.NewLine +
                                Newtonsoft.Json.JsonConvert.SerializeObject(_group, Newtonsoft.Json.Formatting.Indented) + Environment.NewLine +
                                ex.Message + " " + ex.StackTrace
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

                        //allrecs.Add(new Dictionary<string, object>(rec));
                        allrecs.Add(rec);

                        if (breakOtherRecords)
                        {

                            break;
                        }

                    }

                }
                groupByKey = null;
                file1.Data = allrecs;
                //return file1;
                //Sorting


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
                                //sorted_file1 = allrecs.OrderBy(x => Convert.ToDecimal(x[firstOrderItem.name]));
                                sorted_file1 = allrecs.OrderBy(x => Convert.ToDecimal(file1.get(firstOrderItem.name,x)));
                            }
                            else
                            {
                                sorted_file1 = allrecs.OrderBy(x => file1.get(firstOrderItem.name, x));
                            }
                        }
                    }
                    else if (firstOrderItem.sortType == SortType.Deccending)
                    {
                        if (fieldTypes.ContainsKey(firstOrderItem.name))
                        {
                            if (fieldTypes[firstOrderItem.name] == 0)//int
                            {
                                sorted_file1 = allrecs.OrderByDescending(x => Convert.ToDecimal(file1.get(firstOrderItem.name, x)));
                            }
                            else
                            {
                                sorted_file1 = allrecs.OrderByDescending(x => file1.get(firstOrderItem.name, x));
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
                                    sorted_file1 = sorted_file1.ThenBy(x => Convert.ToDecimal(file1.get(firstOrderItem.name, x)));
                                }
                                else
                                {
                                    sorted_file1 = sorted_file1.ThenBy(x => file1.get(firstOrderItem.name, x));
                                }
                            }

                        }
                        else if (item.Value.sortType == SortType.Deccending)
                        {
                            if (fieldTypes.ContainsKey(item.Key))
                            {
                                if (fieldTypes[item.Key] == 0)//int
                                {
                                    sorted_file1 = sorted_file1.ThenByDescending(x => Convert.ToDecimal(file1.get(firstOrderItem.name, x)));
                                }
                                else
                                {
                                    sorted_file1 = sorted_file1.ThenByDescending(x => file1.get(firstOrderItem.name, x));
                                }
                            }

                        }
                    }
                }
                else
                {
                    sorted_file1 = allrecs.OrderBy(x => 1);
                }


                //apply rules
                if (applyRules)
                {
                    var rules = db.fieldRules.Where(p => p.WorkingSetItemId == fileid).OrderBy(p => p.Order).ToList();
                    //update rules as part of fieldType
                    foreach (var rule in rules)
                    {
                        fieldTypes.Add(rule.Name, rule.Type);
                    }
                    file1.Data = sorted_file1.ToList();
                    CallFunction(rules, file1);
                    rules = null;

                }
                
                
            }

            //sorted_file1 = null;
            GC.Collect();
            return file1; ;
        }
        private static string getIfNull(string[] shouldHaveField, Dictionary<string, object> dic)
        {
            var str = "";
            for (int i = 0; i < shouldHaveField.Length; i++)
            {
                if (dic.ContainsKey(shouldHaveField[i]))
                {
                    str += dic[shouldHaveField[i]];
                }
                //str += dic[shouldHaveField[i]];
                //if (!string.IsNullOrEmpty(str))
                //{
                //    return str;
                //}
            }
            return str;
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
            public string delimiter { get; set; }
        }

        private static DynaExp dyna = new DynaExp();
        private static void CallFunction(List<FieldRule> rules, BL._CSV sorted_file1)
        {
            foreach (var rule in rules)
            {
                sorted_file1.Name_index.Add(rule.Name, sorted_file1.Name_index.Max(p => p.Value) + 1);
            }
            var dt = new System.Data.DataTable();
            foreach (var rule in rules)
            {

                if (rule.Type == 0)//math
                {
                    foreach (var rec in sorted_file1.Data)
                    {
                        //rec.Add(rule.Name, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));
                        rec.Add(dt.Compute(rule.ExpValue.FormatWith(rec), "").ToString());

                    }
                }
                else if (rule.Type == 2)//bool
                {
                    foreach (var rec in sorted_file1.Data)
                    {
                        rec.Add(dyna.IS(rule.ExpValue.FormatWith(rec)).ToString());


                    }
                }
                else if (rule.Type == 1)//string
                {
                    foreach (var rec in sorted_file1.Data)
                    {
                        rec.Add(dyna.FORMAT(rule.ExpValue.FormatWith(rec)));


                    }
                }
                else if (rule.Type == 3)//FUNC_Num
                {
                    foreach (var rec in sorted_file1.Data)
                    {
                        rec.Add(dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)).ToString());


                    }
                }
                else if (rule.Type == 4)//obj AS_IS/IF
                {
                    foreach (var rec in sorted_file1.Data)
                    {
                        rec.Add(dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)).ToString());


                    }
                }

            }
            dt.Dispose();
            dt = null;
        }
        private static void CallFunction(IOrderedQueryable<FieldRule> rules, IOrderedEnumerable<Dictionary<string, object>> sorted_file1)
        {

            using (var dt = new System.Data.DataTable())
            {
                foreach (var rule in rules)
                {
                    if (rule.Type == 0)//math
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));


                        }
                    }
                    else if (rule.Type == 2)//bool
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.IS(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 1)//string
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 3)//FUNC_Num
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 4)//obj AS_IS/IF
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));


                        }
                    }

                }
            }
        }
        private static void CallFunction(List<FieldRule> rules, IOrderedEnumerable<Dictionary<string, object>> sorted_file1)
        {

            using (var dt = new System.Data.DataTable())
            {
                foreach (var rule in rules)
                {
                    if (rule.Type == 0)//math
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));


                        }
                    }
                    else if (rule.Type == 2)//bool
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.IS(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 1)//string
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 3)//FUNC_Num
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));


                        }
                    }
                    else if (rule.Type == 4)//obj AS_IS/IF
                    {
                        foreach (var rec in sorted_file1)
                        {
                            rec.Add(rule.Name, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));


                        }
                    }

                }
            }
        }
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
                        }
                        else if (rule.Type == 2)//bool
                        {
                            rec.Add(rule.Name, dyna.IS(rule.ExpValue.FormatWith(rec)));
                        }
                        else if (rule.Type == 1)//string
                        {
                            rec.Add(rule.Name, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));
                        }
                        else if (rule.Type == 3)//FUNC_Num
                        {
                            rec.Add(rule.Name, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));
                        }
                        else if (rule.Type == 4)//obj AS_IS/IF
                        {
                            rec.Add(rule.Name, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
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
        public static string KnownDelimeter(string path)
        {
            var info = GetHeaderInfo(path);
            if (info == null) return string.Empty;
            return info.Delimeter;
        }
        public class _CSVHeader
        {
            public int Count { get; set; }
            public string Delimeter { get; set; }
            public string[] Header { get; set; }
        }
        public static _CSVHeader GetHeaderInfo(string path)
        {
            using (TextReader reader = System.IO.File.OpenText(path))
            {
                var delimeters = new string[] { "\t", ";", ",", ":", "." };
                //reader.Read();
                var header_line = reader.ReadLine();
                var first_rec = reader.ReadLine();
                reader.Close();
                reader.Dispose();
                foreach (var delimeter in delimeters)
                {
                    var char_ = new string[] { delimeter };
                    var header_count = header_line.Split(char_, StringSplitOptions.None);
                    if (header_count.Count() != 1)
                    {
                        var rec_count = first_rec.Split(char_, StringSplitOptions.None);
                        if (header_count.Count() == rec_count.Count())
                        {
                            reader.Close();
                            reader.Dispose();
                            return new _CSVHeader
                            {
                                Count = header_count.Count(),
                                Delimeter = delimeter

                            };
                            //return delimeter;
                        }
                    }

                }
            }
            return null;


        }
    }
}
