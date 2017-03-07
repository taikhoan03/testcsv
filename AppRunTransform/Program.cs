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
                    Console.WriteLine("processing ws: " + req.WorkingSetId);
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
        public static string runProcess(int id, bool cleanUpResult = false)
        {
            
            
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id);
            //if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            var limit =  2 * 1000 * 1000 * 1000;


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
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final(item.First().sndId, limit: limit, addSequence: false, applyRules: false);
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
                             select p.Concat(g == null ? new Dictionary<string, object>() : g).ToDictionary(x => x.Key, x => x.Value);
                    //select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g).ToDictionary(x => x.Key, x => x.Value);
                    //TODO: slow here
                    all_rec = ff.ToList();// new List<IDictionary<string, object>>(ff);// ff.ToDictionary(x=>x.Keys).ToList();
                    loadF1 = null;
                    loadF2 = null;
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
            // apply rule mapper
            
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
            var outputData_ = outputDataWithNameList.GroupBy(c => c.OutputFieldId).ToList();

            var rule_ = rules.ToList();
            //rename field in rule expression
            foreach (var rule in rule_)
            {
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }
            
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
                ls_outputFieldName[index]=fieldname;//.Add(fieldname);
                ls_outputDataDetail.Add(fieldname, rulesForThisField);
                ls_mappers[index] =mapper;//.Add(mapper);
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
            using (var dt = new System.Data.DataTable())
            {
                Console.WriteLine(DateTime.Now.ToShortTimeString());
                Console.WriteLine("Applying Rules...");
                //TODO: nếu ko viết Rule, và chỉ có 1 field dc chọn để map
                foreach (var rec in all_rec)
                {
                    for (int i = 0; i < ls_outputFieldName.Length; i++)
                    {
                        var fieldname = ls_outputFieldName[i];
                        var mapper = ls_mappers[i];
                        var rulesForThisField = ls_outputDataDetail[fieldname];
                        var numOfFields = ls_numOfFields[i];
                        var inputType = ls_isSimpleInputType[i];
                        if (inputType == 2)
                        {
                            var _name = mapper.FileMapperName + ":" + mapper.FieldMapperName;
                            _name = _name.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
                            rec.Add(mapper.FieldName, rec[_name]);
                        }else if (inputType == 1)
                        {
                            rec.Add(mapper.FieldName, string.Empty);
                        }
                        else
                        {
                            foreach (var rule in rulesForThisField)
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
                                        //var a= rule.ExpValue;
                                        //var a = rule.ExpValue.FormatWith(rec);
                                        //icount++;
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
                                    if (rule == rulesForThisField.Last())
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
                    
                    
                }
                Console.WriteLine("-----" + icount);
                Console.WriteLine("Done apply Rules: "+DateTime.Now.ToShortTimeString());
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
                    }else
                        primaryKey = firstFileId.Filename.Replace(".", EV.DOT) + EV.DOLLAR + firstFileId.PrimaryKey;
                }


                //var group1 = _ls.ToList().GroupBy(p => p[primaryKey]);
                //add sequence
                Console.WriteLine("Grouping and adding sequence");
                #region Sequence
                var seqType = 0;
                var fileHasSeq1_only = new string[] { "Land","Land Use", "Assessor Ownership","Sales","Situs Address", "Parcel to Parcel Cross Reference", "Assessor Land Values" };
                var fileHasSeq2 = new string[] { "Assessor Building Values","Assessor Exemption Type", "Building" };
                var fileHasSeq3 = new string[] {  "Building Permit","Building Green Code","Extra Feature","Building Area" };
                var outputPrimaryKey = "UNFORMATTED_APN";
                var firstRec = all_rec.First();
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


                        all_rec = addSeq2(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);



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
                        all_rec = addSeq2(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);
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
                        all_rec = addSeq2(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, hasSndMapped, true);
                        
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
                        var hasThrField=firstRec.ContainsKey(thrFieldKey);
                        var mustHaveFieldFiltered = mustHaveField.Where(p => firstRec.ContainsKey(p));
                        
                        all_rec = addSeq3(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
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

                        all_rec = addSeq3(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);
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
                        
                        
                        all_rec = addSeq3(outputPrimaryKey,all_rec, mustHaveFieldFiltered.ToArray(),sndFieldKey,thrFieldKey, hasSndMapped,hasThrMapped,true, hasThrField);
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

                        all_rec = addSeq3(outputPrimaryKey, all_rec, mustHaveFieldFiltered.ToArray(), sndFieldKey, thrFieldKey, hasSndMapped, hasThrMapped, true, hasThrField);


                    }
                    #endregion Building Area

                }
                #endregion SEQ3
                

                #endregion

                Console.WriteLine("Transforming data");
                dtAll = Ulti.ToDataTable(all_rec);
                //remove columns
                Console.WriteLine("Cleaning result");
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
                var outputDic = outputFields.Where(c => colFields.Any(d => d == c.Name)).ToDictionary(x => x.Name, x => x);
                foreach (DataRow row in dtAll.Rows)
                {
                    foreach (var colName in colFields)
                    {
                        var fieldInfo = outputDic[colName];
                        var cell = row[colName];
                        var content = cell.ToString();
                        if (fieldInfo.Type == EV.TYPE_NUM)
                        {
                            try
                            {
                                if (!string.IsNullOrEmpty(content))
                                    cell = Math.Round(Convert.ToDecimal(cell), fieldInfo.Decimal);
                            }
                            catch (Exception ex)
                            {

                                throw new Exception("Binding driver field FAIL:column=" + colName + ", value=" + content + Environment.NewLine
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
                                cell = content.Substring(0, fieldInfo.Length);
                            }
                        }
                    }
                }
                Console.WriteLine("Writing file");
                
                var name = DateTime.Now.ToString("yyyyMMdd") + "_" + fileOutput.Name + "_" + ws.User + ".csv";
                Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
                    ws.State + "\\" + ws.County + "\\" + name, dtAll);
                all_rec.Clear();
                all_rec = null;
                dtAll.Clear();
                dtAll.Dispose();
                GC.Collect();
                Console.WriteLine("-----------------------------");
                return name;
            }
        }
        private static void TranferColumnsToRecord()
        {

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
                                    //rec[sortField.Key] = _group.FirstOrDefault()[sortField.Key];
                                    rec[sortField.Key] = r_first[sortField.Key];
                                }

                            }
                            else if (action == DuplicateAction.PickupLastValue)
                            {
                                if (!hasKeepAllRows) { breakOtherRecords = true; }
                                //var v = _group.LastOrDefault()[sortField.Key];
                                foreach (var rec in _group)
                                {
                                    //rec[sortField.Key] = _group.LastOrDefault()[sortField.Key];
                                    rec[sortField.Key] = r_last[sortField.Key];

                                }

                            }
                            else if (action == DuplicateAction.PickupFirstUn_NULL_value)
                            {
                                if (!hasKeepAllRows) breakOtherRecords = true;
                                //var v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                                foreach (var rec in _group)
                                {
                                    //rec[sortField.Key] = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
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

                            throw new Exception("ProcessFinal_Sorting_FAIL at: " + sortField.Key
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

                var sorted_file1 = Enumerable.Empty<Dictionary<string, object>>().OrderBy(x => 1);
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
        }

        private static DynaExp dyna = new DynaExp();
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
    }
}
