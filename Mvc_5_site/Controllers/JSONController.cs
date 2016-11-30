using CsvHelper;
using Libs;
using NCalc;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Linq.Dynamic;
using BL;
namespace Mvc_5_site.Controllers
{
    [AllowCrossSiteJson]
    //[System.Web.Http.Cors.EnableCors(origins: "*", headers: "*", methods: "*")]
    public class JSONController : Controller
    {
        public string testString()
        {
            return "OK";
        }
        // GET: JSON
        //[AllowCrossSiteJson]
        //[System.Web.Http.Cors.EnableCors(origins: "*", headers: "*", methods: "*")]
        public JsonResult GetState(string term)
        {
            var _term = term.ToLower();
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process")
                                    );
            var states_folders = Directory.GetDirectories(path).Select(p => (new FileInfo(p)).Name).Where(p => p.ToLower().Contains(_term));
            return Json(states_folders, JsonRequestBehavior.AllowGet);
        }
        public JsonResult GetCounty(string state, string term)
        {
            var _term = term.ToLower();
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state
                                    );
            var states_folders = Directory.GetDirectories(path).Select(p => (new FileInfo(p)).Name).Where(p => p.ToLower().Contains(_term));
            return Json(states_folders, JsonRequestBehavior.AllowGet);
        }

        public JsonResult GetHeader(string state, string county, string filename, string delimeter = "\t")
        {
            var _term = filename.ToLower();
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            //var fileinfo = new System.IO.FileInfo(path + @"\" + filename);
            //fileinfo.
            using (TextReader reader = System.IO.File.OpenText(path + @"\" + filename))
            {
                //reader.Read();
                var header_line = reader.ReadLine();
                var header = header_line.Split(new string[] { delimeter }, StringSplitOptions.RemoveEmptyEntries);
                reader.Close();
                reader.Dispose();
                return Json(header, JsonRequestBehavior.AllowGet);
            }
            //var files = Directory.GetFiles(path, "*.*");
            //if (files.Count() > 0)
            //{

            //        //return Json(files.Select(p => Path.GetFileName(p)).Where(p => p.ToLower() == _term).Select(p => new BL.file
            //        //{
            //        //    Name = p,
            //        //    County = county,

            //        //    State = state,
            //        //}), JsonRequestBehavior.AllowGet);
            //    }

            //return Json(files, JsonRequestBehavior.AllowGet);
        }
        public JsonResult GetFileInfo(string state, string county, string filename, string delimeter = "\t")
        {
            var _term = filename.ToLower();
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            using (TextReader reader = System.IO.File.OpenText(path + @"\" + filename))
            {
                //reader.Read();
                var header_line = reader.ReadLine();
                var header = header_line.Split(new string[] { delimeter }, StringSplitOptions.RemoveEmptyEntries);
                reader.Close();
                reader.Dispose();
                return Json(new { header = header }, JsonRequestBehavior.AllowGet);
            }

        }

        public JsonResult GetSample(string state, string county, string filename)
        {

            var _term = filename.ToLower();
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            //var fileinfo = new System.IO.FileInfo(path + @"\" + filename);
            //fileinfo.
            var delimeter = KnownDelimeter(path + @"\" + filename);
            using (TextReader reader = System.IO.File.OpenText(path + @"\" + filename))
            {
                //reader.Read();
                var arr = new List<string[]>();
                string line;
                //for (int i = 0; (i < 1000) && (reader.ReadLine()!=null); i++)
                //{
                //    line = reader.ReadLine();
                //    var header = line.Split(new string[] { delimeter }, StringSplitOptions.RemoveEmptyEntries);
                //    arr.Add(header);
                //}
                var counter = 0;
                while ((line = reader.ReadLine()) != null)
                {
                    var header = line.Split(new string[] { delimeter }, StringSplitOptions.RemoveEmptyEntries);
                    if (counter == 0)
                    {
                        header = header.Select(p => p.ReplaceUnusedCharacters()).ToArray();
                    }
                    arr.Add(header);
                    counter++;
                    if (counter > 100)
                        break;
                }

                reader.Close();
                reader.Dispose();
                return Json(arr, JsonRequestBehavior.AllowGet);
            }
            //var files = Directory.GetFiles(path, "*.*");
            //if (files.Count() > 0)
            //{

            //        //return Json(files.Select(p => Path.GetFileName(p)).Where(p => p.ToLower() == _term).Select(p => new BL.file
            //        //{
            //        //    Name = p,
            //        //    County = county,

            //        //    State = state,
            //        //}), JsonRequestBehavior.AllowGet);
            //    }

            //return Json(files, JsonRequestBehavior.AllowGet);
        }
        public JsonResult GetFileWithState(string state, string term)
        {
            var _term = term.ToLower();
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state
                                    );
            var files = DirSearch2(path).Select(p => new { path = p.Key, name = Path.GetFileName(p.Value) });
            if (files.Count() > 0)
                return Json(files.Where(p => p.name.ToLower().Contains(_term)).Select(p => new BL.file
                {
                    Name = p.name,
                    County = p.path.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Skip(1).FirstOrDefault(),

                    State = state,
                }), JsonRequestBehavior.AllowGet);
            return Json(files, JsonRequestBehavior.AllowGet);
        }
        public JsonResult GetFileWithStateAndCounty(string state, string county, string term)
        {
            var _term = term.ToLower();
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            var files = Directory.GetFiles(path, "*.*");
            if (files.Count() > 0)
                return Json(files.Select(p => Path.GetFileName(p)).Where(p => p.ToLower().Contains(_term)).Select(p => new BL.file
                {
                    Name = p,
                    County = county,

                    State = state,
                }), JsonRequestBehavior.AllowGet);
            return Json(files, JsonRequestBehavior.AllowGet);
        }
        private List<String> DirSearch(string sDir)
        {
            var files = new List<String>();
            try
            {
                foreach (string f in Directory.GetFiles(sDir))
                {
                    files.Add(f);
                }
                foreach (string d in Directory.GetDirectories(sDir))
                {
                    files.AddRange(DirSearch(d));
                }
            }
            catch (System.Exception excpt)
            {
                //MessageBox.Show(excpt.Message);
            }

            return files;
        }
        private Dictionary<string, string> DirSearch2(string sDir)
        {
            //List<String> files = new List<String>();
            var files = new Dictionary<string, string>();
            try
            {
                foreach (string f in Directory.GetFiles(sDir))
                {
                    var path = sDir + f;
                    var arr = path.Split(new string[] { "\\" }, StringSplitOptions.RemoveEmptyEntries).Reverse().FirstOrDefault();
                    files.Add(arr, f);
                    //files.Add(f);
                }
                foreach (string d in Directory.GetDirectories(sDir))
                {
                    foreach (string f in Directory.GetFiles(d))
                    {
                        var path = d + f;
                        var arr = path.Split(new string[] { "\\" }, StringSplitOptions.RemoveEmptyEntries).Reverse().Take(2);
                        ;
                        files.Add(string.Join(",", arr), f);
                        //files.Add(f);
                    }
                }
            }
            catch (System.Exception excpt)
            {
                //MessageBox.Show(excpt.Message);
            }

            return files;
        }

        public JsonResult GetTestLayoutResult(int workingSetItemid, string state, string county, string filename)
        {
            var db = new BL.DA_Model();
            //var job = db.workingSets.Find(workingSetItemid);
            var columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == workingSetItemid);
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            path = path + @"\" + filename;
            var dic = Helpers.ReadCSV.Read(path, 50);
            var array = dic.ToArray();//.Take(50)



            var columns_exp = columns;//.Select(p=> "''" + p.Mapper);// new string[] { "{KEY_3}", "''+({Major}+3)+'_'+{Minor}" };//db column expression
            var rs = new List<string[]>();


            //create header first
            var header = new List<string>();
            foreach (var item in columns_exp)
            {
                header.Add(item.Fieldname);
            }
            rs.Add(header.ToArray());
            foreach (var rec in array)
            {
                var rec_ = new List<string>();
                IDictionary<string, object> myUnderlyingObject = (ExpandoObject)rec;
                foreach (var item in columns_exp)
                {
                    try
                    {
                        var value = (item.Mapper).FormatWith_ExpandObject(rec);
                        //var value = item.Mapper.Eval_ExpandObject(rec).ToString();
                        rec_.Add(value);

                        if (myUnderlyingObject.Keys.SingleOrDefault(p => p == item.Fieldname) == null)
                            myUnderlyingObject.Add(item.Fieldname, value);
                    }
                    catch (Exception ex)
                    {
                        return Json(ex.ExceptionMessageDetails("Error at: [" + item.Fieldname + "], mapper value: " + item.Mapper), JsonRequestBehavior.AllowGet); ;
                    }

                }
                rs.Add(rec_.ToArray());
            }




            #region call function from string

            #endregion
            return Json(rs, JsonRequestBehavior.AllowGet);


        }
        [HttpPost]
        public string KnownDelimeter(string path)
        {
            var info = GetHeaderInfo(path);
            if (info == null) return string.Empty;
            return info.Delimeter;
        }
        [HttpPost]
        public _CSVHeader GetHeaderInfo(string path)
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
                            return new _CSVHeader {
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
        [HttpPost]
        public string addHeader(string[] headers, string path)
        {
            var headerInfo = GetHeaderInfo(path);
            if (headers.Count() != headerInfo.Count)
            {
                return "Header count not match, much be " + headerInfo.Count + " items";
                //throw new Exception("Header count not match, much be "+ headerInfo.Count+" items");
            }
            var _deli = headerInfo.Delimeter;
            if (string.IsNullOrEmpty(_deli)) return string.Empty;
            var header_line = string.Join(_deli, headers);
            using (var writer = new StreamWriter(path + "___.txt"))
            {
                using (var reader = new StreamReader(path))
                {
                    writer.WriteLine(header_line);
                    while (!reader.EndOfStream)
                        writer.WriteLine(reader.ReadLine());
                }
            }
            return string.Empty;
            //System.IO.File.Copy(path + "___.txt", path + "___.txt", true);
        }
        public string testRule(int workjobid, string state, string county, string filename)
        {
            var rules = new List<BL.Rule>();
            rules.Add(new BL.Rule
            {
                Name = "Rule_1",
                NamedContent = "{Minor}+{Major}+2-{Major}",
                Type = BL.RuleType.Arithmetic
            });
            rules.Add(new BL.Rule
            {
                Name = "Rule_2",
                NamedContent = "{Rule_1}+\"abc\"",
                Type = BL.RuleType.Fortmatting
            });
            rules.Add(new BL.Rule
            {
                Name = "Rule_3",
                NamedContent = "{Rule_2}+\"def\"",
                Type = BL.RuleType.Fortmatting
            });
            rules.Add(new BL.Rule
            {
                Name = "Rule_4",
                NamedContent = "{Rule_1}%2==0",
                Type = BL.RuleType.Conditional
            });


            var db = new BL.DA_Model();
            var job = db.workingJobs.Find(workjobid);
            var columns = db.jobFileLayouts.Where(p => p.WorkingSetItemId == workjobid);
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            path = path + @"\" + filename;
            var dic = Helpers.ReadCSV.Read(path, 1000);
            var records = dic.ToArray();//.Take(50)



            var columns_exp = columns;
            foreach (var item in columns_exp)
            {

                foreach (var rec in records)
                {
                    IDictionary<string, object> myUnderlyingObject = (ExpandoObject)rec;
                    var value = item.Mapper.FormatWith_ExpandObject(rec);
                    if (!myUnderlyingObject.ContainsKey(item.Fieldname))//.Keys.FirstOrDefault(p => p == item.Fieldname) == null)
                        myUnderlyingObject.Add(item.Fieldname, value);
                    else
                    {
                        myUnderlyingObject[item.Fieldname] = value;
                    }
                    //rs.Add(rec);
                }

            }

            ///Apply rules
            var target = new DynamicExpresso.Interpreter();

            foreach (var rule in rules)
            {
                foreach (var rec in records)
                {
                    IDictionary<string, object> myUnderlyingObject = rec;
                    var placeholders = rule.NamedContent.GetPlaceHolderName_ExpandObject();

                    var fullRuleText = "";
                    var _params = new List<DynamicExpresso.Parameter>();
                    foreach (var ph in placeholders.Distinct())
                    {
                        if (fullRuleText == "")
                            fullRuleText = rule.NamedContent.Replace("{" + ph + "}", ph);
                        else
                            fullRuleText = fullRuleText.Replace("{" + ph + "}", ph);
                        var value = myUnderlyingObject[ph];
                        //target.SetVariable(ph, value);
                        var param = new DynamicExpresso.Parameter(ph, value.GetType(), value);
                        _params.Add(param);
                    }
                    var c = _params.ToArray();

                    //TODO: dòng này xữ lý chậm
                    var rule_result = target.Eval(fullRuleText, _params.ToArray());
                    myUnderlyingObject.Add(rule.Name, rule_result);
                }



            }
            var testRS = records;


            return "OK";
        }
        public void test()
        {
            var file1 = Helpers.ReadCSV.Read(@"D:\FA_in_out\InputFile\State 1\Bibb\File_1.txt", 5);
            var _file1 = new BL.CSV_data { Name = "File_1.txt", Records = file1 };
            var file2 = Helpers.ReadCSV.Read(@"D:\FA_in_out\InputFile\State 1\Bibb\File_2.txt", 5);
            var _file2 = new BL.CSV_data { Name = "File_2.txt", Records = file2 };
            var file3 = Helpers.ReadCSV.Read(@"D:\FA_in_out\InputFile\State 1\Bibb\File_3.txt", 5);
            var _file3 = new BL.CSV_data { Name = "File_3.txt", Records = file3 };
            var _csv = new BL.CSV();
            //var listFile = new List<ExpandoObject[]>();
            //listFile.Add(file1.ToArray());
            //listFile.Add(file2.ToArray());
            //listFile.Add(file3.ToArray());
            try
            {
                //_csv.merge_files(new List<BL.CSV_data> { _file1, _file2, _file3 });
                _csv.merge_files(new List<BL.CSV_data> { _file1 }, null, null, null);
            }
            catch (Exception ex)
            {

                throw new Exception(ex.ExceptionMessageDetails("Make sure primary key is unique"));
            }

        }
        public void MergeFiles(int id, string primaryKey)
        {
            var db = new BL.DA_Model();
            var job = db.mergeFileJob.FirstOrDefault(p => p.Id == id);
            var names = job.Filenames.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            try
            {
                MergeFiles_(job.State, job.County, names.ToList(), job.MergeDetails.XMLStringToListObject<BL.MergeDetail>(),
                    job.OutputFilename);
                job.Status = BL.JobStatus.Done.ToString();
                job.ErrorDetail = "";
                //sau khi tao file tao record moi vao bang workingSetItem
                var checkOutputIsAlreadyCreate = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == job.WorkingJobId && p.Filename == job.OutputFilename);
                if (checkOutputIsAlreadyCreate == null)
                {
                    var newFileInWorkingSet = new BL.WorkingSetItem();
                    newFileInWorkingSet.Filename = job.OutputFilename;
                    newFileInWorkingSet.PrimaryKey = primaryKey;
                    newFileInWorkingSet.SecondaryKeys = "";
                    newFileInWorkingSet.WorkingSetId = (int)job.WorkingJobId;
                    db.workingSetItems.Add(newFileInWorkingSet);
                }


            }
            catch (Exception ex)
            {
                job.Status = BL.JobStatus.Fail.ToString();
                job.ErrorDetail = ex.extractErrorMessage();
            }
            finally
            {
                db.SaveChanges();
            }

            //var state = ws.State;
            //var county = ws.County;
            ////var mergeDetails = ws.MergeDetails;///
            //var files = db.workingSetItems.Where(p => p.WorkingSetId == WorkingSetID);
        }

        public void MergeFiles_(string state, string county, List<string> filenames, List<BL.MergeDetail> mergeDetails, string outputFilename)
        {
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            var lsFiles = new List<BL.CSV_data>();
            foreach (var filename in filenames)
            {
                var _path = path + @"\" + filename;
                var file = Helpers.ReadCSV.Read(_path, 0);
                var _file = new BL.CSV_data { Name = filename, Records = file };
                lsFiles.Add(_file);
            }

            var _csv = new BL.CSV();
            //var listFile = new List<ExpandoObject[]>();
            //listFile.Add(file1.ToArray());
            //listFile.Add(file2.ToArray());
            //listFile.Add(file3.ToArray());
            try
            {
                //_csv.merge_files(new List<BL.CSV_data> { _file1, _file2, _file3 });
                _csv.merge_files(lsFiles, mergeDetails, path: path, outputfilename: outputFilename);
            }
            catch (Exception ex)
            {

                throw new Exception(ex.ExceptionMessageDetails("Make sure primary key is unique"));
            }
        }
        public void test_groupby()
        {
            var primaryKey = "PARCEL_NUMBER";
            var concatField = "TAX_DESCRIPTION_LINE";
            var tab = "\t";
            var file1 = Helpers.ReadCSV.Read(@"D:\FA_in_out\InputFile\Tax_description_TAB.txt", 0);

            var group1 = file1.Select(p => (IDictionary<string, object>)p).GroupBy(p => p[primaryKey]);

            var group2 = group1.Select(g => new { g.Key, TAX_DESCRIPTION_LINE = string.Join(" ", g.Select(i => i[concatField])) });

            var sb = new System.Text.StringBuilder();

            var header = primaryKey + tab + concatField + Environment.NewLine;
            sb.Append(header);
            foreach (var item in group2)
            {
                sb.Append(item.Key + tab + item.TAX_DESCRIPTION_LINE + Environment.NewLine);
            }
            System.IO.StreamWriter file = new System.IO.StreamWriter(@"D:\hereIam.txt");
            file.Write(sb.ToString());
            file.Close();
            file.Dispose();
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
            None=0,
            Asccending=1,
            Deccending=2
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
        public JsonResult GetSampleWithSortAndDuplicateAction(int fileid, int limit = 100)
        {
            //int limit = 100;
            var tab = "\t";
            var db = new BL.DA_Model();
            var wsFile = db.workingSetItems.FirstOrDefault(p => p.Id == fileid);
            var ws = db.workingSets.FirstOrDefault(p => p.Id == wsFile.WorkingSetId);

            var sortAndActions = db.fieldOrderAndActions.Where(p => p.WorkingSetItemId == fileid);
            var fields_sort = sortAndActions.OrderBy(x => x.Order)
                .ToDictionary(x => x.FieldName, x => new SortField
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
            var fieldTypes = db.jobFileLayouts.Where(p => p.WorkingSetItemId == fileid).ToDictionary(p=>p.Fieldname,p=>p.Type);
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    ws.State,
                                    ws.County
                                    );
            path= path + @"\" + wsFile.Filename;
            var file1 = Helpers.ReadCSV.ReadAsDictionary(path, limit);
            var primaryKey = wsFile.PrimaryKey.ReplaceUnusedCharacters();
            var group1 = file1.ToList().GroupBy(p => p[primaryKey]);

            var allrecs = new List<IDictionary<string, object>>();


            foreach (var _group in group1)
            {
                //foreach (var record in _group)
                //{

                //}
                var breakOtherRecords = false;
                var ignoreAll = false;
                var record = _group.FirstOrDefault();
                foreach (var sortField in fields_sort)
                {
                    var action = sortField.Value.duplicateAction;
                    try
                    {
                        
                        if (action == DuplicateAction.ResponseWithError)
                        {
                            throw new Exception("ResponseWithError");
                        }
                        else if (action == DuplicateAction.PickupFirstValue)
                        {
                            breakOtherRecords = true;
                            break;
                            //ignoreAll = true;
                            //break;
                        }
                        else if (action == DuplicateAction.PickupLastValue)
                        {
                            breakOtherRecords = true;
                            record[sortField.Key] = _group.LastOrDefault()[sortField.Key];
                            //ignoreAll = true;
                            break;
                        }
                        else if (action == DuplicateAction.PickupFirstUn_NULL_value)
                        {
                            breakOtherRecords = true;
                            record[sortField.Key] = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            //ignoreAll = true;
                            break;
                        }
                        else if (action == DuplicateAction.PickupMaximumValue)
                        {
                            breakOtherRecords = true;
                            record[sortField.Key] = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                            //ignoreAll = true;
                            break;
                        }
                        else if (action == DuplicateAction.PickupMinimumValue)
                        {
                            breakOtherRecords = true;
                            record[sortField.Key] = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            //ignoreAll = true;
                            break;
                        }
                        else if (action == DuplicateAction.SumAllRow)
                        {
                            breakOtherRecords = true;
                            record[sortField.Key] = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
                            //ignoreAll = true;
                            break;
                        }
                        //TODO: ConcatenateWithDelimiter phải xác định delimeter
                        else if (action == DuplicateAction.ConcatenateWithDelimiter)
                        {
                            breakOtherRecords = true;
                            record[sortField.Key] = string.Join(",", _group.Select(i => i[sortField.Key]));
                            //ignoreAll = true;
                            break;
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

                    allrecs.Add(rec);

                    if (breakOtherRecords)
                        break;
                }

            }

            //foreach (var _group in group1)
            //{
            //    //foreach (var record in _group)
            //    //{

            //    //}
            //    var breakOtherRecords = false;
            //    var ignoreAll = false;
            //    var record = _group.FirstOrDefault();
            //    foreach (var sortField in fields_sort)
            //    {
            //        try
            //        {
            //            if (sortField.duplicateAction == DuplicateAction.ResponseWithError)
            //            {
            //                throw new Exception("ResponseWithError");
            //            }
            //            else if (sortField.duplicateAction == DuplicateAction.PickupFirstValue)
            //            {
            //                breakOtherRecords = true;
            //                break;
            //                //ignoreAll = true;
            //                //break;
            //            }
            //            else if (sortField.duplicateAction == DuplicateAction.PickupLastValue)
            //            {
            //                breakOtherRecords = true;
            //                record[sortField.name] = _group.LastOrDefault()[sortField.name];
            //                //ignoreAll = true;
            //                break;
            //            }
            //            else if (sortField.duplicateAction == DuplicateAction.PickupFirstUn_NULL_value)
            //            {
            //                breakOtherRecords = true;
            //                record[sortField.name] = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.name].ToString()))[sortField.name];
            //                //ignoreAll = true;
            //                break;
            //            }
            //            else if (sortField.duplicateAction == DuplicateAction.PickupMaximumValue)
            //            {
            //                breakOtherRecords = true;
            //                record[sortField.name] = _group.Max(p => Convert.ToDecimal(p[sortField.name]));
            //                //ignoreAll = true;
            //                break;
            //            }
            //            else if (sortField.duplicateAction == DuplicateAction.PickupMinimumValue)
            //            {
            //                breakOtherRecords = true;
            //                record[sortField.name] = _group.Min(p => Convert.ToDecimal(p[sortField.name]));
            //                //ignoreAll = true;
            //                break;
            //            }
            //            else if (sortField.duplicateAction == DuplicateAction.SumAllRow)
            //            {
            //                breakOtherRecords = true;
            //                record[sortField.name] = _group.Sum(p => Convert.ToDecimal(p[sortField.name]));
            //                //ignoreAll = true;
            //                break;
            //            }
            //            //TODO: ConcatenateWithDelimiter phải xác định delimeter
            //            else if (sortField.duplicateAction == DuplicateAction.ConcatenateWithDelimiter)
            //            {
            //                breakOtherRecords = true;
            //                record[sortField.name] = string.Join(",", _group.Select(i => i[sortField.name]));
            //                //ignoreAll = true;
            //                break;
            //            }
            //        }
            //        catch (Exception ex)
            //        {

            //            throw new Exception("FAIL at: " + sortField.name
            //                + ", sortType: " + (DuplicateAction)sortField.duplicateAction + Environment.NewLine + "Record: " + Environment.NewLine +
            //                Newtonsoft.Json.JsonConvert.SerializeObject(_group, Newtonsoft.Json.Formatting.Indented));
            //        }
            //    }
            //    foreach (var rec in _group)
            //    {
            //        if (ignoreAll)
            //            break;

            //        allrecs.Add(rec);

            //        if (breakOtherRecords)
            //            break;
            //    }

            //}
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
                        }else
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
            }else
            {
                sorted_file1 = allrecs.OrderBy(x => 1);
            }
             
            
            group1 = sorted_file1.ToList().GroupBy(p => p[primaryKey]);
            //add sequence

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
            var rules = db.fieldRules.Where(p => p.WorkingSetItemId == fileid).OrderBy(p=>p.Order);
            //update rules as part of fieldType
            foreach (var rule in rules)
            {
                //var dicField = new Dictionary<string, int>();
                fieldTypes.Add(rule.Name, rule.Type);
            }
            //var target = new DynamicExpresso.Interpreter();
            var dyna = new DynaExp();
            var dt = new System.Data.DataTable();
            foreach (var rule in rules)
            {
                if (rule.Type == 0)
                {
                    foreach (var rec in sorted_file1)
                    {
                        IDictionary<string, object> myUnderlyingObject = rec;
                        var rule_result = rule.ExpValue.FormatWith(rec);
                        //TODO: dòng này xữ lý chậm
                        myUnderlyingObject.Add(rule.Name, dt.Compute(rule_result,""));// target.Eval(rule_result));

                        
                    }
                }else if (rule.Type == 2)//bool
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
                //foreach (var rec in sorted_file1)
                //{
                //    IDictionary<string, object> myUnderlyingObject = rec;
                //    var placeholders = rule.ExpValue.GetPlaceHolderName_ExpandObject();

                //    var fullRuleText = "";
                //    var _params = new List<DynamicExpresso.Parameter>();
                //    foreach (var ph in placeholders.Distinct())
                //    {
                //        if (fullRuleText == "")
                //            fullRuleText = rule.ExpValue.Replace("{" + ph + "}", ph);
                //        else
                //            fullRuleText = fullRuleText.Replace("{" + ph + "}", ph);
                //        var value = new object();
                //        if (fieldTypes[ph] == 0)
                //        {
                //            value = Convert.ToDecimal(myUnderlyingObject[ph]);
                //        }else
                //        {
                //            value = myUnderlyingObject[ph];
                //        }
                //        //target.SetVariable(ph, value);
                //        var param = new DynamicExpresso.Parameter(ph, value.GetType(), value);
                //        _params.Add(param);
                //    }
                //    var c = _params.ToArray();

                //    //TODO: dòng này xữ lý chậm
                //    var rule_result = target.Eval(fullRuleText, _params.ToArray());
                //    myUnderlyingObject.Add(rule.Name, rule_result);
                //}



            }
            //bool rules
            
            //foreach (var rec in sorted_file1)
            //{
            //    IDictionary<string, object> myUnderlyingObject = rec;
            //    var rule_result = dyna.IS("GREATER_THAN({RULE_3},-7)".FormatWith(rec));
            //    //TODO: dòng này xữ lý chậm
            //    myUnderlyingObject.Add("testBoolRule", rule_result);


            //}


            //generate json data

            var rs = new List<string[]>();
            var header = sorted_file1.Count() != 0 ?
                    sorted_file1.FirstOrDefault().Select(p => p.Key) : allrecs.FirstOrDefault().Select(p => p.Key);
            rs.Add(header.ToArray());
            if (sorted_file1.Count() != 0)
                foreach (var rec in sorted_file1)
                {
                    rs.Add(rec.Select(p => p.Value.ToString()).ToArray());
                }
            else
                foreach (var rec in allrecs)
                {
                    rs.Add(rec.Select(p => p.Value.ToString()).ToArray());
                }
            return Json(rs, JsonRequestBehavior.AllowGet);
            //var sb = new System.Text.StringBuilder("");
            //sb.Append(string.Join(tab, header) + Environment.NewLine);
            //foreach (var rec in allrecs)
            //{
            //    sb.Append(string.Join(tab, rec.Select(p => p.Value)) + Environment.NewLine);
            //}
            //return sb.ToString();
        }
        public void test_groupby_keySort(int num)
        {
            var primaryKey = "PARCEL_NUMBER";
            var concatField = "TAX_DESCRIPTION_LINE";
            var fields_sort = new List<SortField>();
            var sort_key1 = new SortField { name="LINE_NUMBER", duplicateAction=DuplicateAction.PickupLastValue,sortType=SortType.Asccending };
            var sort_key2 = new SortField { name = "TAX_DESCRIPTION_LINE", duplicateAction = DuplicateAction.PickupFirstUn_NULL_value };
            fields_sort.Add(sort_key1);
            //fields_sort.Add(sort_key2);
            fields_sort = fields_sort.OrderBy(p => p.duplicateAction).ToList();

            var tab = "\t";
            var file1 = Helpers.ReadCSV.ReadAsDictionary(@"D:\FA_in_out\InputFile\Tax_description_TAB.txt", num);
            //var arr_sort = new List<string>();
            ////var str_sort = primaryKey + " ASC";
            //arr_sort.Add(primaryKey + " ASC");
            //foreach (var item in fields_sort)
            //{
            //    if (item.sortType == SortType.Accending)
            //    {
            //        arr_sort.Add(item.name + " ASC");
            //    }
            //    else if (item.sortType == SortType.Deccending)
            //    {
            //        arr_sort.Add(item.name + " DESC");
            //    }
            //}
            //var str_sort = string.Join(",", arr_sort);
            //group1 = group1.OrderBy(str_sort);
            //var sorted_file1 = file1.OrderBy(x => Convert.ToDecimal(x[primaryKey]));//.Select(p => (IDictionary<string, object>)p)
            var sorted_file1 = file1.OrderBy(x => x[primaryKey].ToString());//.Select(p => (IDictionary<string, object>)p)
            foreach (var item in fields_sort)
            {
                if (item.sortType == SortType.Asccending)
                {
                    sorted_file1 = sorted_file1.ThenBy(x => x[item.name]);
                }
                else if (item.sortType == SortType.Deccending)
                {
                    sorted_file1 = sorted_file1.ThenByDescending(x => x[item.name]);
                }
            }
            var group1 = sorted_file1.ToList().GroupBy(p => p[primaryKey]);//Select(p => (IDictionary<string, object>)p)
            var allrecs = new List<IDictionary<string, object>>();
            
            foreach (var _group in group1)
            {
                //foreach (var record in _group)
                //{

                //}
                var breakOtherRecords = false;
                var ignoreAll = false;
                var record = _group.FirstOrDefault();
                foreach (var sortField in fields_sort)
                {
                    if (sortField.duplicateAction == DuplicateAction.ResponseWithError)
                    {
                        throw new Exception("ResponseWithError");
                    }

                    //else if(sortField.duplicateAction == DuplicateAction.KeepAllRows)
                    //{
                    //    breakOtherRecords = false;
                    //    break;
                    //}
                    //else if (sortField.duplicateAction == DuplicateAction.DropAllRows)
                    //{
                    //    //breakOtherRecords = true;
                    //    ignoreAll = true;
                    //    break;
                    //}
                    else if (sortField.duplicateAction == DuplicateAction.PickupFirstValue)
                    {
                        breakOtherRecords = true;
                        
                        //ignoreAll = true;
                        break;
                    }
                    else if (sortField.duplicateAction == DuplicateAction.PickupLastValue)
                    {
                        breakOtherRecords = true;
                        record[sortField.name] = _group.LastOrDefault()[sortField.name];
                        //ignoreAll = true;
                        break;
                    }
                    else if (sortField.duplicateAction == DuplicateAction.PickupFirstUn_NULL_value)
                    {
                        breakOtherRecords = true;
                        record[sortField.name] = _group.FirstOrDefault(p=>!string.IsNullOrEmpty(p[sortField.name].ToString()))[sortField.name];
                        //ignoreAll = true;
                        break;
                    }
                    else if (sortField.duplicateAction == DuplicateAction.PickupMaximumValue)
                    {
                        breakOtherRecords = true;
                        record[sortField.name] = _group.Max(p=>Convert.ToDecimal(p[sortField.name]));
                        //ignoreAll = true;
                        break;
                    }
                    else if (sortField.duplicateAction == DuplicateAction.PickupMinimumValue)
                    {
                        breakOtherRecords = true;
                        record[sortField.name] = _group.Min(p => Convert.ToDecimal(p[sortField.name]));
                        //ignoreAll = true;
                        break;
                    }
                    else if (sortField.duplicateAction == DuplicateAction.SumAllRow)
                    {
                        breakOtherRecords = true;
                        record[sortField.name] = _group.Sum(p => Convert.ToDecimal(p[sortField.name]));
                        //ignoreAll = true;
                        break;
                    }
                    //TODO: ConcatenateWithDelimiter phải xác định delimeter
                    else if (sortField.duplicateAction == DuplicateAction.ConcatenateWithDelimiter)
                    {
                        breakOtherRecords = true;
                        record[sortField.name] = string.Join(",", _group.Select(i => i[sortField.name]));
                        //ignoreAll = true;
                        break;
                    }



                    //if (sortField[1] == "0")
                    //{
                    //    record[sortField[0]] = _group.Last()[sortField[0]];
                    //    breakOtherRecords = true;
                    //}
                    //else if (sortField[1] == "1")
                    //{
                    //    record[sortField[0]] = _group.Last()[sortField[0]];

                    //}
                }
                foreach (var rec in _group)
                {
                    if (ignoreAll)
                        break;

                    allrecs.Add(rec);

                    if (breakOtherRecords)
                        break;
                }
                
            }
            //regroup
            sorted_file1 = allrecs.OrderBy(x => x[primaryKey].ToString());//.Select(p => (IDictionary<string, object>)p)
            foreach (var item in fields_sort)
            {
                if (item.sortType == SortType.Asccending)
                {
                    sorted_file1 = sorted_file1.ThenBy(x => x[item.name]);
                }
                else if (item.sortType == SortType.Deccending)
                {
                    sorted_file1 = sorted_file1.ThenByDescending(x => x[item.name]);
                }
            }
            group1 = sorted_file1.ToList().GroupBy(p => p[primaryKey]);
            //add sequence

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
            //test tach cot thanh dong`
            var rs = breakColumnsIntoRecords(sorted_file1.ToList(),"newField");
            rs = test_divide(sorted_file1.ToList(), "LINE_NUMBER", "divided_LineNUmber",2);
            var sb = new System.Text.StringBuilder();

            //var header = primaryKey + tab + concatField + Environment.NewLine;
            //sb.Append(header);
            //foreach (var item in group2)
            //{
            //    sb.Append(item.Key + tab + item.TAX_DESCRIPTION_LINE + Environment.NewLine);
            //}
            //System.IO.StreamWriter file = new System.IO.StreamWriter(@"D:\hereIam.txt");
            //file.Write(sb.ToString());
            //file.Close();
            //file.Dispose();
        }
        public class Param_TransferColumnsIntoRecords
        {
            /// <summary>
            /// new field with name will be generated
            /// </summary>
            public string newFieldName { get; set; }
            /// <summary>
            /// Columns will break in to record
            /// </summary>
            public List<string> columns { get; set; }
        }
        public static List<IDictionary<string, object>> breakColumnsIntoRecords(List<IDictionary<string, object>> ls,string newFieldName)
        {
            var rs = new List<IDictionary<string, object>>();
            var columns = new List<string>() { "LINE_NUMBER", "TAX_DESCRIPTION_LINE" };
            foreach (var rec in ls)
            {
                
                foreach (var columnToTransfer in columns)
                {
                    var newRec = new Dictionary<string, object>(rec);
                    newRec.Add(newFieldName, rec[columnToTransfer]);
                    //newRec.Remove(columnToTransfer);
                    rs.Add(newRec);
                }
            }
            foreach (var rec in rs)
            {

                foreach (var columnToTransfer in columns)
                {
                    //var newRec = new Dictionary<string, object>(rec);
                    //newRec.Add(newFieldName, rec[columnToTransfer]);
                    rec.Remove(columnToTransfer);
                }
            }
            return rs;
        }
        public static List<IDictionary<string, object>> test_divide(List<IDictionary<string, object>> ls, string divide_field, string new_divide_field_name, int divide_num)
        {
            var rs = new List<IDictionary<string, object>>();
            foreach (var rec in ls)
            {
                var divided_value = Convert.ToDecimal(rec[divide_field]) / divide_num;
                for (int i = 0; i < divide_num; i++)
                {
                    var newRec = new Dictionary<string, object>(rec);
                    newRec.Add(new_divide_field_name, divided_value);
                    rs.Add(newRec);
                }
                
            }
            
            return rs;
        }
        public static List<IDictionary<string, object>> test_Sum_column(List<IDictionary<string, object>> ls, string []columns, string new_column_name)
        {
            var rs = new List<IDictionary<string, object>>();
            foreach (var rec in ls)
            {
                //var divided_value = Convert.ToDecimal(rec[divide_field]) / divide_num;
                decimal sumValue = 0;                
                foreach (var column in columns)
                {
                    sumValue += Convert.ToDecimal(rec[column]);
                }
                rec.Add(new_column_name, sumValue);
            }

            return rs;
        }
    }
    
    public class _CSVHeader
    {
        public int Count { get; set; }
        public string Delimeter { get; set; }
        public string[] Header { get; set; }
    }
}