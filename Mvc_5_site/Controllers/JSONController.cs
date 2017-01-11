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
using System.Data;

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
            ResponseWithError        = 0,
            KeepAllRows              = 1,
            PickupFirstValue         = 2,
            PickupLastValue          = 3,
            PickupFirstUn_NULL_value = 4,
            PickupMaximumValue       = 5,
            PickupMinimumValue       = 6,
            SumAllRow                = 7,
            ConcatenateWithDelimiter = 8,
            DropAllRows              = 9,
            
            
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
        public JsonResult GetSampleWithSortAndDuplicateAction(int fileid, decimal limit = 100, bool writeFile=false,int showLimit=1000)
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
            if(string.IsNullOrEmpty(primaryKey))
            {
                throw new Exception("No Primary Key, Please select 1 first");
            }

            var group1 = file1.ToList().GroupBy(p => p[primaryKey]);

            var allrecs = new List<IDictionary<string, object>>();

            var sortActions = fields_sort.OrderBy(p => p.Value.duplicateAction);
            foreach (var _group in group1)
            {
                //foreach (var record in _group)
                //{

                //}
                var breakOtherRecords = false;
                var ignoreAll = false;
                var record = _group.FirstOrDefault();
                var hasKeepAllRows = sortActions.Where(p => p.Value.duplicateAction == DuplicateAction.KeepAllRows).Count()>0;
                foreach (var sortField in sortActions)
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
                            if(!hasKeepAllRows) breakOtherRecords = true;
                            //record[sortField.Key] = _group.FirstOrDefault()[sortField.Key];
                            var v = _group.FirstOrDefault()[sortField.Key];
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;
                                
                            }
                            //break;
                            //ignoreAll = true;
                            
                        }
                        else if (action == DuplicateAction.PickupLastValue)
                        {
                            if (!hasKeepAllRows) { breakOtherRecords = true; }
                            //applyAfterKeepAll = true;
                            //record[sortField.Key] = _group.LastOrDefault()[sortField.Key];
                            var v = _group.LastOrDefault()[sortField.Key];
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }

                        }
                        else if (action == DuplicateAction.PickupFirstUn_NULL_value)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            //applyAfterKeepAll = true;
                            //record[sortField.Key] = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            var v = _group.FirstOrDefault(p => !string.IsNullOrEmpty(p[sortField.Key].ToString()))[sortField.Key];
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.PickupMaximumValue)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            //applyAfterKeepAll = true;
                            //record[sortField.Key] = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                            var v = _group.Max(p => Convert.ToDecimal(p[sortField.Key]));
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.PickupMinimumValue)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            //applyAfterKeepAll = true;
                            //record[sortField.Key] = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            var v = _group.Min(p => Convert.ToDecimal(p[sortField.Key]));
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.SumAllRow)
                        {
                            if (!hasKeepAllRows) breakOtherRecords = true;
                            //applyAfterKeepAll = true;
                            //record[sortField.Key] = _group.Sum(p => Convert.ToDecimal(p[sortField.Key]));
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
                            //applyAfterKeepAll = true;
                            //record[sortField.Key] = string.Join(",", _group.Select(i => i[sortField.Key]));
                            var v = string.Join(",", _group.Select(i => i[sortField.Key]));
                            foreach (var rec in _group)
                            {
                                rec[sortField.Key] = v;

                            }
                        }
                        else if (action == DuplicateAction.KeepAllRows)
                        {
                            breakOtherRecords = false;
                            //record[sortField.Key] = string.Join(",", _group.Select(i => i[sortField.Key]));
                            //ignoreAll = true;
                            //break;
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

                    rec.Add("isDuplicated", 0);// = 0;
                    var numOfPrimaryKeyFound = _group.Count();
                    rec.Add("numOfPrimaryKeyFound", numOfPrimaryKeyFound);
                    if (numOfPrimaryKeyFound > 1)
                    {
                        rec["isDuplicated"] = 1;
                    }

                    allrecs.Add(rec);

                    if (breakOtherRecords)
                    {
                        
                        break;
                    }
                        
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
            //write file
            if(writeFile)
                Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") +"\\"+Config.Data.GetKey("tmp_folder_process")+"\\"+ws.State+"\\"+ws.County+"\\"+wsFile.Filename, sorted_file1.ToList());
            return Json(rs.Take(showLimit), JsonRequestBehavior.AllowGet);
            //var sb = new System.Text.StringBuilder("");
            //sb.Append(string.Join(tab, header) + Environment.NewLine);
            //foreach (var rec in allrecs)
            //{
            //    sb.Append(string.Join(tab, rec.Select(p => p.Value)) + Environment.NewLine);
            //}
            //return sb.ToString();
        }
        private IEnumerable<IDictionary<string,object>> Process(int fileid, decimal limit = 100, bool writeFile = false, int showLimit = 1000, bool addSequence=true,bool applyRules=true)
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
            var fieldTypes = db.jobFileLayouts.Where(p => p.WorkingSetItemId == fileid).ToDictionary(p => p.Fieldname, p => p.Type);
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    ws.State,
                                    ws.County
                                    );
            path = path + @"\" + wsFile.Filename;
            var file1 = Helpers.ReadCSV.ReadAsDictionary(wsFile.Filename, path, limit);
            var primaryKey = wsFile.Filename.Replace(".",EV.DOT)+EV.DOLLAR+wsFile.PrimaryKey.ReplaceUnusedCharacters();
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

                    rec.Add(wsFile.Filename+ EV.DOLLAR + "isDuplicated", 0);// = 0;
                    var numOfPrimaryKeyFound = _group.Count();
                    rec.Add(wsFile.Filename + EV.DOLLAR + "numOfPrimaryKeyFound", numOfPrimaryKeyFound);
                    if (numOfPrimaryKeyFound > 1)
                    {
                        if(isResponseWithError)
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
            if(addSequence)
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

                }
            }
            
            return sorted_file1;
        }
        public void testLinkage3(int id)
        {

            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);

            if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            var dic = new Dictionary<string, IEnumerable<IDictionary<string, object>>>();
            var limit = 200000;// 2*1000*1000*1000;
                               //var ff_result = Enumerable.Empty<Enumerable.Empty<KeyValuePair<string, object>>>();//<IEnumerable<KeyValuePair<string, object>>>();
                               //var FF_result = new List<IEnumerable<KeyValuePair<string, object>>>();


            var files = new List<int>();
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
            var ls = new List<IEnumerable<IDictionary<string, object>>>();
            var allRec = new List<IDictionary<string, object>>();
            //var insRec = new Dictionary<string, object>();

            //foreach (var item in files)
            //{
            //    var loadF1 = Process(item, limit: 1, addSequence: false, applyRules: false);
            //    var firstF1 = loadF1.First();

            //    foreach (var col in firstF1.Keys)
            //    {
            //        if (!insRec.ContainsKey(col))
            //        {
            //            insRec.Add(col, "");
            //        }
            //    }
            //}

            var groupLinkageData = linkageData.GroupBy(p => p.firstId + p.sndId);
            string key = groupLinkageData.First().First().firstField;
            string sndKey = groupLinkageData.First().Last().firstField;
            foreach (var item in groupLinkageData)
            {
                var FF_result = new List<IDictionary<string, object>>();
                var loadF1 = Process(item.First().firstId, limit: limit, addSequence: false, applyRules: false);

                var loadF2 = Process(item.First().sndId, limit: limit, addSequence: false, applyRules: false);

                var firstF1 = loadF1.First();
                var firstF2 = loadF2.First();



                //missing field nếu ko join dc
                var ff = from p in loadF1
                         join pp in loadF2
                         on new { a = p[item.First().firstField], b = p[item.Last().firstField] } equals new { a = pp[item.First().sndField], b = pp[item.Last().sndField] }
                         into ps
                         from g in ps//.DefaultIfEmpty()
                         select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g);// g.Where(kvp => !p.ContainsKey(kvp.Key)));//


                foreach (var gg in ff)
                {
                    var new_rec = new Dictionary<string,object>(); //insRec.ToDictionary(x => x.Key, x => x.Value);// 
                    foreach (var col in gg)
                    {

                        if (!new_rec.ContainsKey(col.Key))
                        {
                            new_rec.Add(col.Key, col.Value);
                        }
                        else
                        {
                            new_rec[col.Key] = col.Value;
                        }
                    }
                    //new_rec[col.] = col.Value;
                    //FF_result.Add(new_rec.ToDictionary(x => x.key, x => x.value));
                    FF_result.Add(new_rec);

                }
                foreach (var f1 in loadF1)
                {
                    f1.Clear();
                }
                foreach (var f1 in loadF2)
                {
                    f1.Clear();
                }
                //foreach (var gg in ff)
                //{
                //    var new_rec = new List<KeyValue>(); //insRec.ToDictionary(x => x.Key, x => x.Value);// 
                //    foreach (var col in gg)
                //    {

                //        if (new_rec.FirstOrDefault(x => x.key == col.Key) == null)
                //        {
                //            new_rec.Add(KeyValue.createNew(col.Key, col.Value));
                //        }
                //        //if (!new_rec.ContainsKey(col.Key))
                //        //{
                //        //    new_rec.Add(col.Key, col.Value);
                //        //}
                //        //else
                //        //{
                //        //    //if (string.IsNullOrEmpty(new_rec[col.Key].ToString()) && !string.IsNullOrEmpty(col.Value.ToString()))
                //        //    new_rec[col.Key] = col.Value;
                //        //}
                //    }
                //    //new_rec[col.] = col.Value;
                //    FF_result.Add(new_rec.ToDictionary(x => x.key, x => x.value));
                //    //FF_result.Add(new_rec);

                //}
                ls.Add(FF_result);
            }


            var lsDataTable = new List<DataTable>();
            foreach (var item in ls)
            {
                lsDataTable.Add(Ulti.ToDataTable(item));
            }
            var dtAll = Ulti.MergeAll(lsDataTable, key);// new DataTable();
            //           var result = ls
            //               .SelectMany(dict => dict)
            //                        //.ToLookup(pair => pair[key], pair => pair)
            //                        .ToLookup(x => Tuple.Create(x[key], x[sndKey]))//pair[key], pair => pair)
            //                        .ToDictionary(group => group.Key, group => group.SelectMany(c => c)
            //                        //.Select(k=>k.fi)
            //                        //.GroupBy(k => k.Key)
            //                        //.Select(k => k.FirstOrDefault(p => !string.IsNullOrEmpty(p[])
            //                        //.Select(k=>k.First())
            //                        //)
            //                        //)
            //);
            //           var a = Ulti.ToDataTable(ls.First().ToList());
            //var aa = result.Select(p => p.Value.ToDictionary(x => x.Key, x => x.Value));//.ToDictionary(x => x.ToDictionary(y=>y.Key), x => x.ToDictionary(y => y.Value));
            //var rs = new List<IDictionary<string, object>>();
            //foreach (var rec in result.Select(p=>p.Value))
            //{
            //    var cols = new Dictionary<string, object>();
            //    //var a = rec;
            //    foreach (var col in rec)
            //    {
            //        if (!cols.ContainsKey(col.Key))
            //            cols.Add(col.Key, col.Value);
            //        else
            //        {
            //            if(string.IsNullOrEmpty(cols[col.Key].ToString()))
            //                cols[col.Key] = col.Value;
            //        }

            //    }
            //    rs.Add(cols);
            //}
            Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("tmp_folder_process") + "\\" +
                "testLinkage.csv", dtAll);
            //foreach (var item in lsDataTable)
            //{
            //    item.Clear();
            //}
            lsDataTable.Clear();
            lsDataTable = null;
            dtAll.Dispose();
            GC.Collect();
        }
        public void testMap(int id,bool cleanUpResult=false)
        {

            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);

            if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            var dic = new Dictionary<string, IEnumerable<IDictionary<string, object>>>();
            var limit = 10000;// 2*1000*1000*1000;
                               //var ff_result = Enumerable.Empty<Enumerable.Empty<KeyValuePair<string, object>>>();//<IEnumerable<KeyValuePair<string, object>>>();
                               //var FF_result = new List<IEnumerable<KeyValuePair<string, object>>>();


            var files = new List<int>();
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
            var ls = new List<IEnumerable<IDictionary<string, object>>>();
            var _ls= new List<IDictionary<string, object>>();
            var allRec = new List<IDictionary<string, object>>();
            


            var groupLinkageData = linkageData.GroupBy(p => p.firstId + p.sndId);
            string key = groupLinkageData.First().First().firstField;
            string sndKey = groupLinkageData.First().Last().firstField;
            var dtAll = new DataTable();
            foreach (var item in groupLinkageData)
            {
                var FF_result = new List<IDictionary<string, object>>();
                var loadF1 = Process(item.First().firstId, limit: limit, addSequence: false, applyRules: false);

                var loadF2 = Process(item.First().sndId, limit: limit, addSequence: false, applyRules: false);

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
                var left1 = item.First().firstFilename.Replace(".",EV.DOT) + EV.DOLLAR + item.First().firstField;
                var right1 = item.Last().firstFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().firstField;
                var left2 = item.First().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.First().sndField;
                var right2 = item.Last().sndFilename.Replace(".", EV.DOT) + EV.DOLLAR + item.Last().sndField;
                var ff = from p in _ls
                         join pp in loadF2
                         on new
                         {
                             a = p[left1],
                             b = p[right1]
                         }
                         equals new
                         {
                             a = pp[left2],
                             b = pp[right2]
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
                _ls = new List<IDictionary<string,object>>(ff);// ff.ToDictionary(x=>x.Keys).ToList();
                
            }
            // apply rule mapper
            var outputFields = db.outputFields.Where(p => p.OutputMapperId == ws.SeletedOutputId);
            var outputData = db.outputDatas.Where(p => outputFields.Any(c => c.Id == p.OutputFieldId));
            var rules = db.outputDataDetails.Where(p => p.OutputFileId == ws.SeletedOutputId).ToList();//.OrderBy(p => p.Order);
            var outputData_ = outputData.ToList().GroupBy(c=>c.OutputFieldId);
            //var outputData_ = new List<BL.OutputData>
            //{
            //    new OutputData
            //    {
            //        OutputFieldId=1,
            //        FileMapperName="Improvement_TAB.txt",
            //        FieldMapperName="FIBS",

            //    },
            //    new OutputData
            //    {
            //        OutputFieldId=2,
            //        FileMapperName="Improvement_TAB.txt",
            //        FieldMapperName="aaa",

            //    }
            //};
            var rule_ = rules.ToList();
            foreach (var rule in rule_)
            {
                rule.ExpValue = rule.ExpValue.Replace(".", EV.DOT).Replace(":", EV.DOLLAR);
            }
            //var rule_ = new List<BL.OutputDataDetail>
            //{
            //    new OutputDataDetail
            //    {
            //        Name="Rule_1",
            //        OutputFieldName="Improvement_TAB.txt$PARCEL_NUMBER",
            //        Type=1,
            //        ExpValue="CONCATENATE({Improvement_TAB.txt$PARCEL_NUMBER}[[]]1)".Replace(".",EV.DOT),
            //        OutputFieldId=1
            //    },
            //    new OutputDataDetail
            //    {
            //        Name="Rule_2",
            //        OutputFieldName="Improvement_TAB.txt$PARCEL_NUMBER",
            //        Type=1,
            //        ExpValue="CONCATENATE({Rule_1}[[]]2)".Replace(".",EV.DOT),
            //        OutputFieldId=1
            //    }
            //};

            var dyna = new DynaExp();
            var dt = new System.Data.DataTable();
            foreach (var rec in _ls)
            {
                IDictionary<string, object> myUnderlyingObject = rec;
                foreach (var group_field in outputData_)
                {

                    var ruleForThisField = rule_.Where(p => p.OutputFieldId == group_field.Key).ToList();
                    var fieldname = group_field.Key+EV.DOLLAR;
                    foreach (var rule in ruleForThisField)
                    {
                        var rule_fullname = fieldname + EV.DOLLAR + rule.Name;
                        if (rule.Type == 0)
                        {
                            //var rule_result = rule.ExpValue.FormatWith(rec);
                            //TODO: dòng này xữ lý chậm
                            myUnderlyingObject.Add(rule_fullname, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));
                        }
                        else if (rule.Type == 2)//bool
                        {
                            //var rule_result = dyna.IS(rule.ExpValue.FormatWith(rec));
                            myUnderlyingObject.Add(rule_fullname, dyna.IS(rule.ExpValue.FormatWith(rec)));
                        }
                        else if (rule.Type == 1)//string
                        {
                            //var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
                            myUnderlyingObject.Add(rule_fullname, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));
                        }
                        else if (rule.Type == 3)//string
                        {
                            //var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
                            myUnderlyingObject.Add(rule_fullname, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));
                        }
                        else if (rule.Type == 4)//string
                        {
                            //var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
                            myUnderlyingObject.Add(rule_fullname, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));
                        }
                        if (rule == ruleForThisField.Last())
                        {
                            //myUnderlyingObject[field.FileMapperName.Replace(".", EV.DOT) + EV.DOLLAR + field.FieldMapperName] =
                            //    myUnderlyingObject[rule.Name];
                            myUnderlyingObject.Add(group_field.First().FieldMapperName, myUnderlyingObject[rule_fullname]);
                        }
                    }
                }
            }

            var a = 1;
            //update rules as part of fieldType
            //foreach (var rule in rules)
            //{
            //    //var dicField = new Dictionary<string, int>();
            //    fieldTypes.Add(rule.Name, rule.Type);
            //}
            //var target = new DynamicExpresso.Interpreter();
            //var firstRec = _ls.First();
            //foreach (var col in firstRec)
            //{
            //    var rule_for_this_field=rules.Where(p=>p.)
            //}


            //var dyna = new DynaExp();
            //var dt = new System.Data.DataTable();
            //foreach (var rule in rules)
            //{
            //    if (rule.Type == 0)
            //    {
            //        foreach (var rec in sorted_file1)
            //        {
            //            IDictionary<string, object> myUnderlyingObject = rec;
            //            var rule_result = rule.ExpValue.FormatWith(rec);
            //            //TODO: dòng này xữ lý chậm
            //            myUnderlyingObject.Add(rule.Name, dt.Compute(rule_result, ""));// target.Eval(rule_result));


            //        }
            //    }
            //    else if (rule.Type == 2)//bool
            //    {
            //        foreach (var rec in sorted_file1)
            //        {
            //            IDictionary<string, object> myUnderlyingObject = rec;
            //            var rule_result = dyna.IS(rule.ExpValue.FormatWith(rec));
            //            //TODO: dòng này xữ lý chậm
            //            myUnderlyingObject.Add(rule.Name, rule_result);


            //        }
            //    }
            //    else if (rule.Type == 1)//string
            //    {
            //        foreach (var rec in sorted_file1)
            //        {
            //            IDictionary<string, object> myUnderlyingObject = rec;
            //            var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
            //            //TODO: dòng này xữ lý chậm
            //            myUnderlyingObject.Add(rule.Name, rule_result);


            //        }
            //    }
            //}

            dtAll = Ulti.ToDataTable(_ls);
            //remove columns
            if (cleanUpResult)
            {
                var list_col_to_remove = new List<DataColumn>();
                foreach (DataColumn col in dtAll.Columns)
                {
                    if (!outputData_.Any(c => c.First().FieldMapperName == col.ColumnName))
                    {
                        list_col_to_remove.Add(col);
                    }
                }
                foreach (var col in list_col_to_remove)
                {
                    dtAll.Columns.Remove(col);
                }
            }
            
            
            Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("tmp_folder_process") + "\\" +
                "testLinkage.csv", dtAll);
            //foreach (var item in lsDataTable)
            //{
            //    item.Clear();
            //}
            //lsDataTable.Clear();
            //lsDataTable = null;
            dtAll.Dispose();
            GC.Collect();
        }
        public void testLinkage2(int id)
        {
            
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);

            if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            var dic = new Dictionary<string, IEnumerable<IDictionary<string, object>>>();
            var limit = 200000;// 2*1000*1000*1000;
            //var ff_result = Enumerable.Empty<Enumerable.Empty<KeyValuePair<string, object>>>();//<IEnumerable<KeyValuePair<string, object>>>();
            //var FF_result = new List<IEnumerable<KeyValuePair<string, object>>>();
            

            var files = new List<int>();
            foreach (var item in linkageData)
            {
                if(files.FirstOrDefault(p=>p==item.firstId) == 0)
                {
                    files.Add(item.firstId);
                }
                if (files.FirstOrDefault(p => p == item.sndId) == 0)
                {
                    files.Add(item.sndId);
                }
            }
            var ls = new List<IEnumerable<IDictionary<string, object>>>();
            var allRec= new List<IDictionary<string, object>>();
            var insRec = new Dictionary<string, object>();
            
            foreach (var item in files)
            {
                var loadF1 = Process(item, limit: 1, addSequence: false, applyRules: false);
                var firstF1 = loadF1.First();

                foreach (var col in firstF1.Keys)
                {
                    if (!insRec.ContainsKey(col))
                    {
                        insRec.Add(col, "");
                    }
                }
            }

            var groupLinkageData = linkageData.GroupBy(p => p.firstId+p.sndId);
            string key = groupLinkageData.First().First().firstField;
            string sndKey = groupLinkageData.First().Last().firstField;
            foreach (var item in groupLinkageData)
            {
                var FF_result = new List<IDictionary<string, object>>();
                var loadF1 = Process(item.First().firstId, limit: limit, addSequence: false, applyRules: false);

                var loadF2 = Process(item.First().sndId, limit: limit, addSequence: false, applyRules: false);

                var firstF1 = loadF1.First();
                var firstF2 = loadF2.First();



                //missing field nếu ko join dc
                var ff = from p in loadF1
                         join pp in loadF2
                         on new { a=p[item.First().firstField] , b=p[item.Last().firstField] } equals new { a=pp[item.First().sndField],b=pp[item.Last().sndField]}
                         into ps
                         from g in ps//.DefaultIfEmpty()
                         select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g);// g.Where(kvp => !p.ContainsKey(kvp.Key)));//


                foreach (var gg in ff)
                {
                    var new_rec = insRec.ToDictionary(x => x.Key, x => x.Value);// new Dictionary<string, object>();
                    foreach (var col in gg)
                    {
                        if (!new_rec.ContainsKey(col.Key))
                        {
                            new_rec.Add(col.Key, col.Value);
                        }
                        else
                        {
                            //if (string.IsNullOrEmpty(new_rec[col.Key].ToString()) && !string.IsNullOrEmpty(col.Value.ToString()))
                            new_rec[col.Key] = col.Value;
                        }
                    }
                    //new_rec[col.] = col.Value;
                    FF_result.Add(new_rec);

                }
                ls.Add(FF_result);
            }

            
            var lsDataTable = new List<DataTable>();
            foreach (var item in ls)
            {
                lsDataTable.Add(Ulti.ToDataTable(item));
            }
            var dtAll = Ulti.MergeAll(lsDataTable, "PARCEL_NUMBER");// new DataTable();
            //           var result = ls
            //               .SelectMany(dict => dict)
            //                        //.ToLookup(pair => pair[key], pair => pair)
            //                        .ToLookup(x => Tuple.Create(x[key], x[sndKey]))//pair[key], pair => pair)
            //                        .ToDictionary(group => group.Key, group => group.SelectMany(c => c)
            //                        //.Select(k=>k.fi)
            //                        //.GroupBy(k => k.Key)
            //                        //.Select(k => k.FirstOrDefault(p => !string.IsNullOrEmpty(p[])
            //                        //.Select(k=>k.First())
            //                        //)
            //                        //)
            //);
            //           var a = Ulti.ToDataTable(ls.First().ToList());
            //var aa = result.Select(p => p.Value.ToDictionary(x => x.Key, x => x.Value));//.ToDictionary(x => x.ToDictionary(y=>y.Key), x => x.ToDictionary(y => y.Value));
            //var rs = new List<IDictionary<string, object>>();
            //foreach (var rec in result.Select(p=>p.Value))
            //{
            //    var cols = new Dictionary<string, object>();
            //    //var a = rec;
            //    foreach (var col in rec)
            //    {
            //        if (!cols.ContainsKey(col.Key))
            //            cols.Add(col.Key, col.Value);
            //        else
            //        {
            //            if(string.IsNullOrEmpty(cols[col.Key].ToString()))
            //                cols[col.Key] = col.Value;
            //        }

            //    }
            //    rs.Add(cols);
            //}
            Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("tmp_folder_process") + "\\" +
                "testLinkage.csv", dtAll);

        }
        public void testLinkage(int id)
        {
            //var f1 = Process(3011,limit:10,addSequence:false,applyRules:false);
            //var f2 = Process(3010, limit: 10, addSequence: false, applyRules: false);
            //var f3 = Process(3009, limit: 10, addSequence: false, applyRules: false);

            //var linkageData = new List<LinkageItem>();
            //linkageData.Add(new LinkageItem
            //{
            //    firstId = 3011,
            //    firstFilename = "File_1.txt",
            //    firstField = "PARCEL_NUMBER",
            //    sndId = 3010,
            //    sndFilename = "File_2.txt",
            //    sndField = "PARCEL_NUMBER"
            //});
            //linkageData.Add(new LinkageItem
            //{
            //    firstId = 3010,
            //    firstFilename = "File_2.txt",
            //    firstField = "PARCEL_NUMBER",
            //    sndId = 3009,
            //    sndFilename = "File_3.txt",
            //    sndField = "PARCELNUMBER"
            //});
            //linkageData.Add(new LinkageItem
            //{
            //    firstId = 3011,
            //    firstFilename = "File_1.txt",
            //    firstField = "PARCEL_NUMBER",
            //    sndId = 3009,
            //    sndFilename = "File_3.txt",
            //    sndField = "PARCELNUMBER"
            //});
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);

            if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            var dic = new Dictionary<string,IEnumerable<IDictionary<string, object>>>();
            var limit = 10000;
            //var ff_result = Enumerable.Empty<Enumerable.Empty<KeyValuePair<string, object>>>();//<IEnumerable<KeyValuePair<string, object>>>();
            //var FF_result = new List<IEnumerable<KeyValuePair<string, object>>>();
            var FF_result = new List<IDictionary<string, object>>();
            foreach (var item in linkageData)
            {
                var loadF1 = Process(item.firstId, limit: limit, addSequence: false, applyRules: false);

                var loadF2 = Process(item.sndId, limit: limit, addSequence: false, applyRules: false);

                var firstF1 = loadF1.First();
                var firstF2 = loadF2.First();
                foreach (var col in firstF2.Keys)
                {
                    if (!firstF1.ContainsKey(col))
                    {
                        foreach (var rec in loadF1)
                        {
                            //var value = rec[item.firstField];
                            //rec.Remove(item.sndField);
                            rec.Add(col, "");
                        }
                    }
                }

                //if (!firstF1.ContainsKey(item.sndField))
                //{
                //    foreach (var rec in loadF1)
                //    {
                //        //var value = rec[item.firstField];
                //        //rec.Remove(item.sndField);
                //        rec.Add(item.sndField, rec[item.firstField]);
                //    }
                //}
                foreach (var col in firstF1.Keys)
                {
                    if (!firstF2.ContainsKey(col))
                    {
                        foreach (var rec in loadF2)
                        {
                            //var value = rec[item.sndField];
                            //rec.Remove(item.sndField);
                            rec.Add(col, "");
                        }
                    }
                }
                //if (!firstF2.ContainsKey(item.firstField))
                //{
                //    foreach (var rec in loadF2)
                //    {
                //        //var value = rec[item.sndField];
                //        //rec.Remove(item.sndField);
                //        rec.Add(item.firstField, rec[item.sndField]);
                //    }
                //}


                //missing field nếu ko join dc
                var ff = from p in loadF1
                         join pp in loadF2
                         on p[item.firstField] equals pp[item.sndField]
                         into ps
                         from g in ps.DefaultIfEmpty()
                         select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() :g);// g.Where(kvp => !p.ContainsKey(kvp.Key)));//


                //FF_result= from p in FF_result
                //           join pp in loadF1
                //           on p. equals pp[item.sndField]
                //           select p.Concat(pp.Where(kvp => !p.ContainsKey(kvp.Key)));
                
                foreach (var gg in ff)
                {
                    var new_rec = new Dictionary<string, object>();
                    foreach (var col in gg)
                    {
                        if (!new_rec.ContainsKey(col.Key))
                        {
                            new_rec.Add(col.Key, col.Value);
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(new_rec[col.Key].ToString()) && !string.IsNullOrEmpty(col.Value.ToString()))
                                new_rec[col.Key] = col.Value;
                        }
                    }
                    
                    FF_result.Add(new_rec);
                    
                }

                //var ff = loadF1.Concat(loadF2);//.GroupBy(d => d.Keys)             .ToDictionary(d => d.Key, d => d.First().Values);

                if (!dic.ContainsKey(item.firstFilename+item.sndFilename))
                    dic.Add(item.firstFilename + item.sndFilename, FF_result);
                //if (!dic.ContainsKey(item.sndFilename))
                //    dic.Add(item.sndFilename, loadF2);
            }
 //           var ls = new List<IEnumerable<IDictionary<string, object>>>();
 //           foreach (var item in dic)
 //           {
 //               ls.Add(item.Value);
 //           }
 //           //{
 //           //    f1,f2,f3
 //           //};
 //           // left outer join
 //           var key = linkageData.First().firstField;
 //           var result = ls
 //               .SelectMany(dict => dict)
 //                        .ToLookup(pair => pair[key], pair => pair)
 //                        .ToDictionary(group => group.Key, group => group.SelectMany(c=>c)
 //                        .GroupBy(k=>k.Key)
 //                        .Select(k=>k.FirstOrDefault(p=>!string.IsNullOrEmpty(p.Value.ToString())))
 //                        //.GroupBy(k => k.Key)
 ////.Where(g => g.Count() > 1)
 ////.Select(g => g)
 //);
           
 //           //.ToDictionary(group => group.Key, group => group.SelectMany(c =>c));
 //           // refilter to left inner join
 //           //var lsResult = from p in dic[linkageData.First().firstFilename]
 //           //               join pp in result
 //           //               on p[key] equals pp.Key
 //           //               select pp.Value
 //           //               ;
 //           var _rs = new List<IDictionary<string, object>>();

 //           //foreach (var rec in lsResult)
 //           //{
 //           //    _rs.Add(rec.ToDictionary(x => x.Key, x => x.Value));
 //           //    //_rs.Add(rec);
 //           //}
 //           foreach (var rec in result)
 //           {
 //               _rs.Add(rec.Value.ToDictionary(x => x.Key, x => x.Value));
 //               //_rs.Add(rec);
 //           }

            Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("tmp_folder_process") + "\\" +
                "testLinkage.csv", FF_result);
            //foreach (var key in result)
            //{
            //    //key.Value.ToDictionary()
            //    //foreach (var item in key.Value)
            //    //{
            //    //    item.Value
            //    //}
            //}
            //var result = f1.Union(f2);
            //var aa=result.ToDictionary(d => d.Keys, d => d.First().Value);
            //var j1=from p1 in f1 
            //       join p2 in f2
            //       on p1["PARCEL_NUMBER"] equals p2["PARCEL_NUMBER"]
            //       into a
            //       from b in a.DefaultIfEmpty()
            //       select new
            //       {
            //           b[""],
            //           Name = bk.BookNm,
            //           b.PaymentMode
            //       };
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
    public class KeyValue
    {
        public string key { get; set; }
        public object value { get; set; }
        public static KeyValue createNew(string key,object value)
        {
            return new KeyValue { key = key, value = value };
        }
    }
}