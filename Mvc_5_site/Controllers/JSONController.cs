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
                var header = header_line.Split(new string[] { KnownDelimeter(path + @"\" + filename) }, StringSplitOptions.RemoveEmptyEntries).Select(p=>p.ReplaceUnusedCharacters());
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
                var header = header_line.Split(new string[] { KnownDelimeter(path + @"\" + filename) }, StringSplitOptions.RemoveEmptyEntries).Select(p=>p.ReplaceUnusedCharacters());
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
            var tab = KnownDelimeter(path);
            var dic = BL.ReadCSV.ReadAsDictionary(path, 50,tab);
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
                IDictionary<string, object> myUnderlyingObject = rec;
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
            var dic = BL.ReadCSV.ReadAsDictionary(path, 1000);
            var records = dic.ToArray();//.Take(50)



            var columns_exp = columns;
            foreach (var item in columns_exp)
            {

                foreach (var rec in records)
                {
                    IDictionary<string, object> myUnderlyingObject = rec;
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
        //public void test()
        //{
        //    var file1 = ReadCSV.Read(@"D:\FA_in_out\InputFile\State 1\Bibb\File_1.txt", 5);
        //    var _file1 = new BL.CSV_data { Name = "File_1.txt", Records = file1 };
        //    var file2 = Helpers.ReadCSV.Read(@"D:\FA_in_out\InputFile\State 1\Bibb\File_2.txt", 5);
        //    var _file2 = new BL.CSV_data { Name = "File_2.txt", Records = file2 };
        //    var file3 = Helpers.ReadCSV.Read(@"D:\FA_in_out\InputFile\State 1\Bibb\File_3.txt", 5);
        //    var _file3 = new BL.CSV_data { Name = "File_3.txt", Records = file3 };
        //    var _csv = new BL.CSV();
        //    //var listFile = new List<ExpandoObject[]>();
        //    //listFile.Add(file1.ToArray());
        //    //listFile.Add(file2.ToArray());
        //    //listFile.Add(file3.ToArray());
        //    try
        //    {
        //        //_csv.merge_files(new List<BL.CSV_data> { _file1, _file2, _file3 });
        //        _csv.merge_files(new List<BL.CSV_data> { _file1 }, null, null, null);
        //    }
        //    catch (Exception ex)
        //    {

        //        throw new Exception(ex.ExceptionMessageDetails("Make sure primary key is unique"));
        //    }

        //}
        public void MergeFiles(int id, string primaryKey)
        {
            var db = new BL.DA_Model();
            var job = db.mergeFileJob.FirstOrDefault(p => p.Id == id);
            var mergeDetails = job.MergeDetails.XMLStringToListObject<BL.MergeDetail>();
            foreach (var item in mergeDetails)
            {
                foreach (var field in item.Fields)
                {
                    field.FieldName = field.FieldName.ReplaceUnusedCharacters();
                }
            }
            var names = job.Filenames.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            try
            {
                MergeFiles_(job.State, job.County, names.ToList(), mergeDetails,
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
                GC.Collect();
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
                var file = ReadCSV.Read(_path, 0);
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
            var file1 = ReadCSV.Read(@"D:\FA_in_out\InputFile\Tax_description_TAB.txt", 0);

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
            public string delimiter { get; set; }
        }
        public JsonResult GetSampleWithSortAndDuplicateAction(int fileid, decimal limit = 100, bool writeFile=false,int showLimit=1000)
        {
            //int limit = 100;
            
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
                    duplicateActionType = x.DuplicatedActionType,
                    delimiter=x.ConcatenateWithDelimiter
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
            var tab = KnownDelimeter(path);
            var file1 = ReadCSV.ReadAsDictionary(path, limit,tab);
            var primaryKey = wsFile.PrimaryKey.ReplaceUnusedCharacters();
            if(string.IsNullOrEmpty(primaryKey))
            {
                throw new Exception("No Primary Key, Please select 1 first");
            }

            var group1 = file1.ToList().GroupBy(p => p[primaryKey]);

            var allrecs = new List<Dictionary<string, object>>();

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
                        var delimiter = sortField.Value.delimiter;
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
                            var deli = "";
                            if (!string.IsNullOrEmpty(delimiter))
                                deli = delimiter;
                            var v = string.Join(deli, _group.Select(i => i[sortField.Key]));
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
            //            var rule_result = rule.ExpValue.FormatWith(rec);
            //            //TODO: dòng này xữ lý chậm
            //            myUnderlyingObject.Add(rule.Name, dt.Compute(rule_result,""));// target.Eval(rule_result));


            //        }
            //    }else if (rule.Type == 2)//bool
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
                ReadCSV.Write(Config.Data.GetKey("root_folder_process") +"\\"+Config.Data.GetKey("tmp_folder_process")+"\\"+ws.State+"\\"+ws.County+"\\"+wsFile.Filename, sorted_file1.ToList());
            GC.Collect();
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
            
            var db = new BL.DA_Model();
            
            //var wsFiles = db.workingSetItems.Where(p => p.Id == fileid);
            var wsFile = db.workingSetItems.FirstOrDefault(p => p.Id == fileid);
            var ws = db.workingSets.FirstOrDefault(p => p.Id == wsFile.WorkingSetId);

            var sortAndActions = db.fieldOrderAndActions.Where(p => p.WorkingSetItemId == fileid).Select(p=>new {
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
                .ToDictionary(x => x.FileName, x => new SortField
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
            var tab = KnownDelimeter(path);
            var file1 = ReadCSV.ReadAsDictionary(wsFile.Filename, path, limit,tab);
            var primaryKey = wsFile.Filename;//.Replace(".",EV.DOT)+EV.DOLLAR+wsFile.PrimaryKey.ReplaceUnusedCharacters();
            if (string.IsNullOrEmpty(primaryKey))
            {
                throw new Exception("No Primary Key, Please select 1 first");
            }

            var group1 = file1.ToList().GroupBy(p => p[primaryKey]);

            var allrecs = new List<Dictionary<string, object>>();

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

                    rec.Add("isDuplicated", 0);// = 0;
                    var numOfPrimaryKeyFound = _group.Count();
                    rec.Add("numOfPrimaryKeyFound", numOfPrimaryKeyFound);
                    if (numOfPrimaryKeyFound > 1)
                    {
                        if(isResponseWithError)
                            throw new Exception("ResponseWithError");
                        rec["isDuplicated"] = 1;

                    }

                    allrecs.Add(rec);

                    if (breakOtherRecords)
                    {

                        break;
                    }

                }

            }

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
                //            myUnderlyingObject.Add(rule.Name, rule_result);


                //        }
                //    }
                //    else if (rule.Type == 1)//string
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            IDictionary<string, object> myUnderlyingObject = rec;
                //            var rule_result = dyna.FORMAT(rule.ExpValue.FormatWith(rec));
                //            myUnderlyingObject.Add(rule.Name, rule_result);


                //        }
                //    }

                //}
            }
            GC.Collect();
            return sorted_file1;
        }
        private IEnumerable<Dictionary<string, object>> Process_final(int fileid, decimal limit = 100, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
        {
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
                var file1 = ReadCSV.ReadAsDictionary(wsFile.Filename, path, limit);
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
        /// <summary>
        /// 
        /// </summary>
        /// <param name="wsiId"></param>
        /// <param name="strcolumns">seperatedBy:";];"</param>
        /// <param name="toNewName"></param>
        /// <param name="newFileName"></param>
        public void TransferColumnsToRecord(int wsiId,string strcolumns,string toNewName, string newFileName)
        {
            var db = new BL.DA_Model();
            var wsItem = db.workingSetItems.Find(wsiId);
            var ws = db.workingSets.Find(wsItem.WorkingSetId);
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                        Config.Data.GetKey("input_folder_process"),
                                        ws.State,
                                        ws.County
                                        );
            path = path + @"\" + wsItem.Filename;

            //test
            //path = @"D:\FA_in_out\InputFile\pa4\pa4\File_1.txt";

            var columns = strcolumns.Split(new string[] { ";];" }, StringSplitOptions.RemoveEmptyEntries);
            var numOfColumns = columns.Length;

            var file1 = ReadCSV.ReadAsDictionary(null, path, int.MaxValue);
            var arrFields = new Dictionary<string, object>(file1.First());


            columns = columns.Select(p => p.ReplaceUnusedCharacters()).ToArray();
            var leftCols = arrFields.Select(p => p.Key).Except(columns).ToArray();
            var numOfLeftItem = leftCols.Count();
            //toNewName = toNewName.ReplaceUnusedCharacters();

            var numOfItems = file1.Count;
            var cloney = new List<Dictionary<string, object>>(numOfItems);//[numOfItems];

            //file1.CopyTo(cloney);
            //cloney=file1.Select(p=>p.Select(c=>))
            
            for (int i = 0; i < numOfItems; i++)
            {
                var c= new Dictionary<string, object>(numOfLeftItem);
                for (int j = 0; j < numOfLeftItem; j++)
                {
                    c.Add(leftCols[j], file1[i][leftCols[j]]);
                }
                cloney.Add(c);
            }
            
            foreach (var rec in file1)
            {
                for (int i = 0; i < numOfLeftItem; i++)
                {
                    rec.Remove(leftCols[i]);
                }
            }
            GC.Collect();
            //file1.Clear();
            var calTotalItem = numOfItems * numOfColumns;
            var tmp = new Dictionary<string, object>[calTotalItem];// List<Dictionary<string, object>>(calTotalItem);
            
            var index = 1;
            for (int i = numOfItems - 1; i >= 0; i--)
            {
                // some code
                // safePendingList.RemoveAt(i);
                var newRec = cloney[i];//.Last();
                var recContainCol = file1[i];
                //var irow = 0;
                for (int j = numOfColumns-1; j >=0; j--)
                {
                    var tmpRec = newRec.ToDictionary(x => x.Key, x => x.Value);
                    tmpRec.Add(toNewName, recContainCol[columns[j]]);
                    tmp[calTotalItem - index] = tmpRec;
                    index++;
                }
                newRec.Clear();
                newRec = null;
                recContainCol.Clear();
                recContainCol = null;

                //file1[i].Clear();
                //file1[i]=null;
                file1.RemoveAt(i);
                //cloney[i].Clear();
                //cloney[i] = null;
                cloney.RemoveAt(i);
            }
            
            cloney.Clear();
            cloney = null;
            file1.Clear();
            file1 = null;


            //var recs_after_add_seqs = tmp;// new List<Dictionary<string, object>>();
            var sndSEQ_FieldName = "seq2".ReplaceUnusedCharacters();
            // file1.First().ToList();
            foreach (var col in columns)
            {
                arrFields.Remove(col);
            }
            var firstRec = arrFields;// file1.First();
            //set primaryKey
            var primaryKeyName = "UNFORMATTED_APN";
            if (!string.IsNullOrEmpty(wsItem.PrimaryKey))
            {
                if (firstRec.ContainsKey(wsItem.PrimaryKey))
                {
                    primaryKeyName = wsItem.PrimaryKey;
                }
            }
            var seq1FieldName = "APN_SEQUENCE_NUMBER";
            var hasSEQ1Field= firstRec.ContainsKey(seq1FieldName.ToString());
            var hasSndField = firstRec.ContainsKey(sndSEQ_FieldName.ToString());
            var dicKey = new Dictionary<string, object>();
            //var seq2 = 1;
            //foreach (var record in tmp)
            //{
            //    if (dicKey.ContainsKey(record[primaryKeyName].ToString()))
            //    {
            //        seq2++;
            //    }
            //    else
            //    {
            //        seq2 = 1;
            //    }

            //    if (hasSEQ1Field)
            //        record[seq1FieldName] = 1;
            //    else
            //        record.Add(seq1FieldName, 1);

            //    if (hasSndField)
            //        record[sndSEQ_FieldName] = seq2;
            //    else
            //        record.Add(sndSEQ_FieldName, seq2);
            //}
            foreach (var gb in tmp.GroupBy(p => p[primaryKeyName].ToString()))
            {
                var seq2 = 1;

                foreach (var record in gb)
                {
                    record.Add("seq1", 1);
                    record.Add("seq2", seq2);
                    //if (hasSEQ1Field)
                    //    record[seq1FieldName] = 1;
                    //else
                    //    record.Add(seq1FieldName, 1);

                    //if (hasSndField)
                    //    record[sndSEQ_FieldName] = seq2;
                    //else
                    //    record.Add(sndSEQ_FieldName, seq2);
                    //record[sndSEQ_FieldName] = seq2;
                    //recs_after_add_seqs.Add(record);



                    seq2++;
                }


            }
                
                BL.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("input_folder_process") + "\\" +
                    ws.State + "\\" + ws.County + "\\" + newFileName, tmp);
            //dtAll.Clear();
            //dtAll.Dispose();
            tmp = null;
            GC.Collect();
            //Helpers.ReadCSV.Write(@"D:\abc.csv", dtAll);


        }
        private void CallFunction(List<FieldRule> rules, BL._CSV sorted_file1)
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
        public void CallFunction(IOrderedQueryable<FieldRule> rules, IOrderedEnumerable<Dictionary<string, object>> sorted_file1)
        {
            var dyna = new DynaExp();
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
        private void CallFunction(List<FieldRule> rules, IOrderedEnumerable<Dictionary<string, object>> sorted_file1)
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
        private void CallFunction(List<FieldRule> rules, IOrderedEnumerable<Dictionary<string, object>> sorted_file1,string namePrefix)
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
                    }
                    var newrec = new Dictionary<string, object>(rec);
                    rec.Clear();
                    foreach (var item in newrec)
                    {
                        //item = new KeyValuePair<string, object>();
                        rec.Add(namePrefix+item.Key, item.Value);
                    }
                }
                //foreach (var rule in rules)
                //{
                //    if (rule.Type == 0)//math
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            rec.Add(rule.Name, dt.Compute(rule.ExpValue.FormatWith(rec), ""));// target.Eval(rule_result));


                //        }
                //    }
                //    else if (rule.Type == 2)//bool
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            rec.Add(rule.Name, dyna.IS(rule.ExpValue.FormatWith(rec)));


                //        }
                //    }
                //    else if (rule.Type == 1)//string
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            rec.Add(rule.Name, dyna.FORMAT(rule.ExpValue.FormatWith(rec)));


                //        }
                //    }
                //    else if (rule.Type == 3)//FUNC_Num
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            rec.Add(rule.Name, dyna.FUNC_Num(rule.ExpValue.FormatWith(rec)));


                //        }
                //    }
                //    else if (rule.Type == 4)//obj AS_IS/IF
                //    {
                //        foreach (var rec in sorted_file1)
                //        {
                //            rec.Add(rule.Name, dyna.FUNC_Obj(rule.ExpValue.FormatWith(rec)));


                //        }
                //    }

                //}
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
        public void testMap(int id,bool cleanUpResult=false)
        {
            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id);


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
            var dyna = new DynaExp();
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
                        }
                        else if (inputType == 1)
                        {
                            rec.Add(mapper.FieldName, null);
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
                Console.WriteLine("Done apply Rules: " + DateTime.Now.ToShortTimeString());
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
                var fileOutput = db.outputMappers.Find(ws.SeletedOutputId);
                var name = DateTime.Now.ToString("yyyyMMdd") + "_" + fileOutput.Name + "_" + ws.User + ".csv";
                ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("output_folder_process") + "\\" +
                    ws.State + "\\" + ws.County + "\\" + name, dtAll);
                all_rec.Clear();
                all_rec = null;
                dtAll.Clear();
                dtAll.Dispose();
                GC.Collect();
                //Console.WriteLine("-----------------------------");
                //return name;
            }
        }

        #region TestLinkage
        //public void testLinkage(int id)
        //{
        //    //var f1 = Process(3011,limit:10,addSequence:false,applyRules:false);
        //    //var f2 = Process(3010, limit: 10, addSequence: false, applyRules: false);
        //    //var f3 = Process(3009, limit: 10, addSequence: false, applyRules: false);

        //    //var linkageData = new List<LinkageItem>();
        //    //linkageData.Add(new LinkageItem
        //    //{
        //    //    firstId = 3011,
        //    //    firstFilename = "File_1.txt",
        //    //    firstField = "PARCEL_NUMBER",
        //    //    sndId = 3010,
        //    //    sndFilename = "File_2.txt",
        //    //    sndField = "PARCEL_NUMBER"
        //    //});
        //    //linkageData.Add(new LinkageItem
        //    //{
        //    //    firstId = 3010,
        //    //    firstFilename = "File_2.txt",
        //    //    firstField = "PARCEL_NUMBER",
        //    //    sndId = 3009,
        //    //    sndFilename = "File_3.txt",
        //    //    sndField = "PARCELNUMBER"
        //    //});
        //    //linkageData.Add(new LinkageItem
        //    //{
        //    //    firstId = 3011,
        //    //    firstFilename = "File_1.txt",
        //    //    firstField = "PARCEL_NUMBER",
        //    //    sndId = 3009,
        //    //    sndFilename = "File_3.txt",
        //    //    sndField = "PARCELNUMBER"
        //    //});
        //    var db = new BL.DA_Model();
        //    var ws = db.workingSets.FirstOrDefault(p => p.Id == id);

        //    if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


        //    var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
        //    var dic = new Dictionary<string, IEnumerable<IDictionary<string, object>>>();
        //    var limit = 10000;
        //    //var ff_result = Enumerable.Empty<Enumerable.Empty<KeyValuePair<string, object>>>();//<IEnumerable<KeyValuePair<string, object>>>();
        //    //var FF_result = new List<IEnumerable<KeyValuePair<string, object>>>();
        //    var FF_result = new List<IDictionary<string, object>>();
        //    foreach (var item in linkageData)
        //    {
        //        var loadF1 = Process(item.firstId, limit: limit, addSequence: false, applyRules: false);

        //        var loadF2 = Process(item.sndId, limit: limit, addSequence: false, applyRules: false);

        //        var firstF1 = loadF1.First();
        //        var firstF2 = loadF2.First();
        //        foreach (var col in firstF2.Keys)
        //        {
        //            if (!firstF1.ContainsKey(col))
        //            {
        //                foreach (var rec in loadF1)
        //                {
        //                    //var value = rec[item.firstField];
        //                    //rec.Remove(item.sndField);
        //                    rec.Add(col, "");
        //                }
        //            }
        //        }

        //        //if (!firstF1.ContainsKey(item.sndField))
        //        //{
        //        //    foreach (var rec in loadF1)
        //        //    {
        //        //        //var value = rec[item.firstField];
        //        //        //rec.Remove(item.sndField);
        //        //        rec.Add(item.sndField, rec[item.firstField]);
        //        //    }
        //        //}
        //        foreach (var col in firstF1.Keys)
        //        {
        //            if (!firstF2.ContainsKey(col))
        //            {
        //                foreach (var rec in loadF2)
        //                {
        //                    //var value = rec[item.sndField];
        //                    //rec.Remove(item.sndField);
        //                    rec.Add(col, "");
        //                }
        //            }
        //        }
        //        //if (!firstF2.ContainsKey(item.firstField))
        //        //{
        //        //    foreach (var rec in loadF2)
        //        //    {
        //        //        //var value = rec[item.sndField];
        //        //        //rec.Remove(item.sndField);
        //        //        rec.Add(item.firstField, rec[item.sndField]);
        //        //    }
        //        //}


        //        //missing field nếu ko join dc
        //        var ff = from p in loadF1
        //                 join pp in loadF2
        //                 on p[item.firstField] equals pp[item.sndField]
        //                 into ps
        //                 from g in ps.DefaultIfEmpty()
        //                 select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g);// g.Where(kvp => !p.ContainsKey(kvp.Key)));//


        //        //FF_result= from p in FF_result
        //        //           join pp in loadF1
        //        //           on p. equals pp[item.sndField]
        //        //           select p.Concat(pp.Where(kvp => !p.ContainsKey(kvp.Key)));

        //        foreach (var gg in ff)
        //        {
        //            var new_rec = new Dictionary<string, object>();
        //            foreach (var col in gg)
        //            {
        //                if (!new_rec.ContainsKey(col.Key))
        //                {
        //                    new_rec.Add(col.Key, col.Value);
        //                }
        //                else
        //                {
        //                    if (string.IsNullOrEmpty(new_rec[col.Key].ToString()) && !string.IsNullOrEmpty(col.Value.ToString()))
        //                        new_rec[col.Key] = col.Value;
        //                }
        //            }

        //            FF_result.Add(new_rec);

        //        }

        //        //var ff = loadF1.Concat(loadF2);//.GroupBy(d => d.Keys)             .ToDictionary(d => d.Key, d => d.First().Values);

        //        if (!dic.ContainsKey(item.firstFilename + item.sndFilename))
        //            dic.Add(item.firstFilename + item.sndFilename, FF_result);
        //        //if (!dic.ContainsKey(item.sndFilename))
        //        //    dic.Add(item.sndFilename, loadF2);
        //    }
        //    //           var ls = new List<IEnumerable<IDictionary<string, object>>>();
        //    //           foreach (var item in dic)
        //    //           {
        //    //               ls.Add(item.Value);
        //    //           }
        //    //           //{
        //    //           //    f1,f2,f3
        //    //           //};
        //    //           // left outer join
        //    //           var key = linkageData.First().firstField;
        //    //           var result = ls
        //    //               .SelectMany(dict => dict)
        //    //                        .ToLookup(pair => pair[key], pair => pair)
        //    //                        .ToDictionary(group => group.Key, group => group.SelectMany(c=>c)
        //    //                        .GroupBy(k=>k.Key)
        //    //                        .Select(k=>k.FirstOrDefault(p=>!string.IsNullOrEmpty(p.Value.ToString())))
        //    //                        //.GroupBy(k => k.Key)
        //    ////.Where(g => g.Count() > 1)
        //    ////.Select(g => g)
        //    //);

        //    //           //.ToDictionary(group => group.Key, group => group.SelectMany(c =>c));
        //    //           // refilter to left inner join
        //    //           //var lsResult = from p in dic[linkageData.First().firstFilename]
        //    //           //               join pp in result
        //    //           //               on p[key] equals pp.Key
        //    //           //               select pp.Value
        //    //           //               ;
        //    //           var _rs = new List<IDictionary<string, object>>();

        //    //           //foreach (var rec in lsResult)
        //    //           //{
        //    //           //    _rs.Add(rec.ToDictionary(x => x.Key, x => x.Value));
        //    //           //    //_rs.Add(rec);
        //    //           //}
        //    //           foreach (var rec in result)
        //    //           {
        //    //               _rs.Add(rec.Value.ToDictionary(x => x.Key, x => x.Value));
        //    //               //_rs.Add(rec);
        //    //           }

        //    Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("tmp_folder_process") + "\\" +
        //        "testLinkage.csv", FF_result);
        //    //foreach (var key in result)
        //    //{
        //    //    //key.Value.ToDictionary()
        //    //    //foreach (var item in key.Value)
        //    //    //{
        //    //    //    item.Value
        //    //    //}
        //    //}
        //    //var result = f1.Union(f2);
        //    //var aa=result.ToDictionary(d => d.Keys, d => d.First().Value);
        //    //var j1=from p1 in f1 
        //    //       join p2 in f2
        //    //       on p1["PARCEL_NUMBER"] equals p2["PARCEL_NUMBER"]
        //    //       into a
        //    //       from b in a.DefaultIfEmpty()
        //    //       select new
        //    //       {
        //    //           b[""],
        //    //           Name = bk.BookNm,
        //    //           b.PaymentMode
        //    //       };
        //}
        //public void testLinkage2(int id)
        //{

        //    var db = new BL.DA_Model();
        //    var ws = db.workingSets.FirstOrDefault(p => p.Id == id);

        //    if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


        //    var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
        //    var dic = new Dictionary<string, IEnumerable<IDictionary<string, object>>>();
        //    var limit = 200000;// 2*1000*1000*1000;
        //                       //var ff_result = Enumerable.Empty<Enumerable.Empty<KeyValuePair<string, object>>>();//<IEnumerable<KeyValuePair<string, object>>>();
        //                       //var FF_result = new List<IEnumerable<KeyValuePair<string, object>>>();


        //    var files = new List<int>();
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
        //    var ls = new List<IEnumerable<IDictionary<string, object>>>();
        //    var allRec = new List<IDictionary<string, object>>();
        //    var insRec = new Dictionary<string, object>();

        //    foreach (var item in files)
        //    {
        //        var loadF1 = Process(item, limit: 1, addSequence: false, applyRules: false);
        //        var firstF1 = loadF1.First();

        //        foreach (var col in firstF1.Keys)
        //        {
        //            if (!insRec.ContainsKey(col))
        //            {
        //                insRec.Add(col, "");
        //            }
        //        }
        //    }

        //    var groupLinkageData = linkageData.GroupBy(p => p.firstId + p.sndId);
        //    string key = groupLinkageData.First().First().firstField;
        //    string sndKey = groupLinkageData.First().Last().firstField;
        //    foreach (var item in groupLinkageData)
        //    {
        //        var FF_result = new List<IDictionary<string, object>>();
        //        var loadF1 = Process(item.First().firstId, limit: limit, addSequence: false, applyRules: false);

        //        var loadF2 = Process(item.First().sndId, limit: limit, addSequence: false, applyRules: false);

        //        var firstF1 = loadF1.First();
        //        var firstF2 = loadF2.First();



        //        //missing field nếu ko join dc
        //        var ff = from p in loadF1
        //                 join pp in loadF2
        //                 on new { a = p[item.First().firstField], b = p[item.Last().firstField] } equals new { a = pp[item.First().sndField], b = pp[item.Last().sndField] }
        //                 into ps
        //                 from g in ps//.DefaultIfEmpty()
        //                 select p.Concat(g == null ? Enumerable.Empty<KeyValuePair<string, object>>() : g);// g.Where(kvp => !p.ContainsKey(kvp.Key)));//


        //        foreach (var gg in ff)
        //        {
        //            var new_rec = insRec.ToDictionary(x => x.Key, x => x.Value);// new Dictionary<string, object>();
        //            foreach (var col in gg)
        //            {
        //                if (!new_rec.ContainsKey(col.Key))
        //                {
        //                    new_rec.Add(col.Key, col.Value);
        //                }
        //                else
        //                {
        //                    //if (string.IsNullOrEmpty(new_rec[col.Key].ToString()) && !string.IsNullOrEmpty(col.Value.ToString()))
        //                    new_rec[col.Key] = col.Value;
        //                }
        //            }
        //            //new_rec[col.] = col.Value;
        //            FF_result.Add(new_rec);

        //        }
        //        ls.Add(FF_result);
        //    }


        //    var lsDataTable = new List<DataTable>();
        //    foreach (var item in ls)
        //    {
        //        lsDataTable.Add(Ulti.ToDataTable(item));
        //    }
        //    var dtAll = Ulti.MergeAll(lsDataTable, "PARCEL_NUMBER");// new DataTable();
        //    //           var result = ls
        //    //               .SelectMany(dict => dict)
        //    //                        //.ToLookup(pair => pair[key], pair => pair)
        //    //                        .ToLookup(x => Tuple.Create(x[key], x[sndKey]))//pair[key], pair => pair)
        //    //                        .ToDictionary(group => group.Key, group => group.SelectMany(c => c)
        //    //                        //.Select(k=>k.fi)
        //    //                        //.GroupBy(k => k.Key)
        //    //                        //.Select(k => k.FirstOrDefault(p => !string.IsNullOrEmpty(p[])
        //    //                        //.Select(k=>k.First())
        //    //                        //)
        //    //                        //)
        //    //);
        //    //           var a = Ulti.ToDataTable(ls.First().ToList());
        //    //var aa = result.Select(p => p.Value.ToDictionary(x => x.Key, x => x.Value));//.ToDictionary(x => x.ToDictionary(y=>y.Key), x => x.ToDictionary(y => y.Value));
        //    //var rs = new List<IDictionary<string, object>>();
        //    //foreach (var rec in result.Select(p=>p.Value))
        //    //{
        //    //    var cols = new Dictionary<string, object>();
        //    //    //var a = rec;
        //    //    foreach (var col in rec)
        //    //    {
        //    //        if (!cols.ContainsKey(col.Key))
        //    //            cols.Add(col.Key, col.Value);
        //    //        else
        //    //        {
        //    //            if(string.IsNullOrEmpty(cols[col.Key].ToString()))
        //    //                cols[col.Key] = col.Value;
        //    //        }

        //    //    }
        //    //    rs.Add(cols);
        //    //}
        //    Helpers.ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("tmp_folder_process") + "\\" +
        //        "testLinkage.csv", dtAll);

        //}
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
                    var new_rec = new Dictionary<string, object>(); //insRec.ToDictionary(x => x.Key, x => x.Value);// 
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
            ReadCSV.Write(Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("tmp_folder_process") + "\\" +
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
        #endregion
        private DynaExp dyna = new DynaExp();
        int x_record_limited_proccess_apply_rules_GC = 4000000;
        public string runProcess(int id, bool cleanUpResult = true, int limit =100)
        {


            var db = new BL.DA_Model();
            var ws = db.workingSets.FirstOrDefault(p => p.Id == id);
            var firstFileId = db.workingSetItems.FirstOrDefault(p => p.WorkingSetId == id);
            //if (string.IsNullOrEmpty(ws.Linkage)) throw new Exception("Empty linkage data");


            var linkageData = ws.Linkage.XMLStringToListObject<LinkageItem>();
            //var limit = 2 * 1000 * 1000 * 1000;


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
                        all_rec = loadF1.ToList();
                    }
                    else
                    {
                        loadF1 = cached2;
                        Console.WriteLine("Get data from file " + item.First().sndFilename);
                        loadF2 = Process_final2(item.First().sndId, limit: limit, addSequence: false, applyRules: true);
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
                    foreach (var _item in ff)
                    {
                        all_rec.Add(new Dictionary<string, object>(_item));
                    }
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
                        loadF1 = Process_final2(onlyRuleForOneFile.Id, limit: limit, addSequence: false, applyRules: true);
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
            using (var dt = new System.Data.DataTable())
            {

                Console.WriteLine("Applying Rules...");
                //TODO: nếu ko viết Rule, và chỉ có 1 field dc chọn để map
                var irec = 0;

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
                    foreach (var record in all_rec)
                    {
                        foreach (var col in list_col_to_remove)
                        {
                            record.Remove(col);
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
                                if (!string.IsNullOrEmpty(content))
                                    record[fieldname] = Math.Round(Convert.ToDecimal(content),
                                        fieldInfo.Decimal);
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

                return Newtonsoft.Json.JsonConvert.SerializeObject(all_rec);
            }
        }
        private IEnumerable<Dictionary<string, object>> Process_final2(int fileid, decimal limit = 100, bool writeFile = false, int showLimit = 1000, bool addSequence = true, bool applyRules = true)
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
                    CallFunction(rules, sorted_file1, wsFile.Filename.Replace(".", EV.DOT) + EV.DOLLAR);
                    rules = null;

                }
                //allrecs.Clear();
                //allrecs = null;

            }
            //GC.Collect();
            return sorted_file1;
        }
        //public void test_groupby_keySort(int num)
        //{
        //    var primaryKey = "PARCEL_NUMBER";
        //    var concatField = "TAX_DESCRIPTION_LINE";
        //    var fields_sort = new List<SortField>();
        //    var sort_key1 = new SortField { name="LINE_NUMBER", duplicateAction=DuplicateAction.PickupLastValue,sortType=SortType.Asccending };
        //    var sort_key2 = new SortField { name = "TAX_DESCRIPTION_LINE", duplicateAction = DuplicateAction.PickupFirstUn_NULL_value };
        //    fields_sort.Add(sort_key1);
        //    //fields_sort.Add(sort_key2);
        //    fields_sort = fields_sort.OrderBy(p => p.duplicateAction).ToList();

        //    var tab = "\t";
        //    var file1 = Helpers.ReadCSV.ReadAsDictionary(@"D:\FA_in_out\InputFile\Tax_description_TAB.txt", num);
        //    //var arr_sort = new List<string>();
        //    ////var str_sort = primaryKey + " ASC";
        //    //arr_sort.Add(primaryKey + " ASC");
        //    //foreach (var item in fields_sort)
        //    //{
        //    //    if (item.sortType == SortType.Accending)
        //    //    {
        //    //        arr_sort.Add(item.name + " ASC");
        //    //    }
        //    //    else if (item.sortType == SortType.Deccending)
        //    //    {
        //    //        arr_sort.Add(item.name + " DESC");
        //    //    }
        //    //}
        //    //var str_sort = string.Join(",", arr_sort);
        //    //group1 = group1.OrderBy(str_sort);
        //    //var sorted_file1 = file1.OrderBy(x => Convert.ToDecimal(x[primaryKey]));//.Select(p => (IDictionary<string, object>)p)
        //    var sorted_file1 = file1.OrderBy(x => x[primaryKey].ToString());//.Select(p => (IDictionary<string, object>)p)
        //    foreach (var item in fields_sort)
        //    {
        //        if (item.sortType == SortType.Asccending)
        //        {
        //            sorted_file1 = sorted_file1.ThenBy(x => x[item.name]);
        //        }
        //        else if (item.sortType == SortType.Deccending)
        //        {
        //            sorted_file1 = sorted_file1.ThenByDescending(x => x[item.name]);
        //        }
        //    }
        //    var group1 = sorted_file1.ToList().GroupBy(p => p[primaryKey]);//Select(p => (IDictionary<string, object>)p)
        //    var allrecs = new List<IDictionary<string, object>>();

        //    foreach (var _group in group1)
        //    {
        //        //foreach (var record in _group)
        //        //{

        //        //}
        //        var breakOtherRecords = false;
        //        var ignoreAll = false;
        //        var record = _group.FirstOrDefault();
        //        foreach (var sortField in fields_sort)
        //        {
        //            if (sortField.duplicateAction == DuplicateAction.ResponseWithError)
        //            {
        //                throw new Exception("ResponseWithError");
        //            }

        //            //else if(sortField.duplicateAction == DuplicateAction.KeepAllRows)
        //            //{
        //            //    breakOtherRecords = false;
        //            //    break;
        //            //}
        //            //else if (sortField.duplicateAction == DuplicateAction.DropAllRows)
        //            //{
        //            //    //breakOtherRecords = true;
        //            //    ignoreAll = true;
        //            //    break;
        //            //}
        //            else if (sortField.duplicateAction == DuplicateAction.PickupFirstValue)
        //            {
        //                breakOtherRecords = true;

        //                //ignoreAll = true;
        //                break;
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
        //                record[sortField.name] = _group.FirstOrDefault(p=>!string.IsNullOrEmpty(p[sortField.name].ToString()))[sortField.name];
        //                //ignoreAll = true;
        //                break;
        //            }
        //            else if (sortField.duplicateAction == DuplicateAction.PickupMaximumValue)
        //            {
        //                breakOtherRecords = true;
        //                record[sortField.name] = _group.Max(p=>Convert.ToDecimal(p[sortField.name]));
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



        //            //if (sortField[1] == "0")
        //            //{
        //            //    record[sortField[0]] = _group.Last()[sortField[0]];
        //            //    breakOtherRecords = true;
        //            //}
        //            //else if (sortField[1] == "1")
        //            //{
        //            //    record[sortField[0]] = _group.Last()[sortField[0]];

        //            //}
        //        }
        //        foreach (var rec in _group)
        //        {
        //            if (ignoreAll)
        //                break;

        //            allrecs.Add(rec);

        //            if (breakOtherRecords)
        //                break;
        //        }

        //    }
        //    //regroup
        //    sorted_file1 = allrecs.OrderBy(x => x[primaryKey].ToString());//.Select(p => (IDictionary<string, object>)p)
        //    foreach (var item in fields_sort)
        //    {
        //        if (item.sortType == SortType.Asccending)
        //        {
        //            sorted_file1 = sorted_file1.ThenBy(x => x[item.name]);
        //        }
        //        else if (item.sortType == SortType.Deccending)
        //        {
        //            sorted_file1 = sorted_file1.ThenByDescending(x => x[item.name]);
        //        }
        //    }
        //    group1 = sorted_file1.ToList().GroupBy(p => p[primaryKey]);
        //    //add sequence

        //    foreach (var _group in group1)
        //    {
        //        var increasement = 1;
        //        foreach (var record in _group)
        //        {
        //            record.Add("seq", 1);
        //            record.Add("seq2", increasement);
        //            increasement++;
        //        }

        //    }
        //    //test tach cot thanh dong`
        //    var rs = breakColumnsIntoRecords(sorted_file1.ToList(),"newField");
        //    rs = test_divide(sorted_file1.ToList(), "LINE_NUMBER", "divided_LineNUmber",2);
        //    var sb = new System.Text.StringBuilder();

        //    //var header = primaryKey + tab + concatField + Environment.NewLine;
        //    //sb.Append(header);
        //    //foreach (var item in group2)
        //    //{
        //    //    sb.Append(item.Key + tab + item.TAX_DESCRIPTION_LINE + Environment.NewLine);
        //    //}
        //    //System.IO.StreamWriter file = new System.IO.StreamWriter(@"D:\hereIam.txt");
        //    //file.Write(sb.ToString());
        //    //file.Close();
        //    //file.Dispose();
        //}
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