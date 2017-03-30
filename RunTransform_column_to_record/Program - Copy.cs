using BL;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RunTransform_column_to_record
{
    class Program
    {
        static void Main(string[] args)
        {
            Config.initFolders();
            Console.WriteLine("App is running...");
            using (Process p = Process.GetCurrentProcess())
                p.PriorityClass = ProcessPriorityClass.BelowNormal;
            var db = new BL.DA_Model();
            while (true)
            {
                //lay req dang chạy trước (trường hợp bị tắt hoặc dừng đột xuất thì chạy lại)
                var req = db.req_Transfer_Columns_to_Records.FirstOrDefault(p => p.Status == 1 && p.IsReady && !p.IsDeleted);
                if (req == null)
                    req = db.req_Transfer_Columns_to_Records.FirstOrDefault(p => p.Status == 0 && p.IsReady && !p.IsDeleted);
                if (req != null)
                {
                    Console.WriteLine("processing ws: " + req.WorkingSetId);
                    req.Status = 1;
                    req.IsReady = true;
                    db.SaveChanges();
                    try
                    {
                        var oldOutputName = req.OutputName;
                        var watch = Stopwatch.StartNew();

                        var wsItem = db.workingSetItems.Find(req.WorkingSetItemId);
                        var ws = db.workingSets.Find(wsItem.WorkingSetId);
                        var downloadedPath=download(ws.State,ws.County,wsItem.Filename);
                        var destinationPath=TransferColumnsToRecord(req.WorkingSetItemId, req.StrColumns, req.New_Field_Name, req.OutputName);
                        upload(ws.State, ws.County, wsItem.Filename,destinationPath);
                        if (!File.Exists(downloadedPath))
                        {
                            File.Delete(downloadedPath);

                        }
                        //var outputname = runProcess(req.WorkingSetId, true);
                        ////remove old file
                        //if (outputname != oldOutputName)
                        //{
                        //    var ws = db.workingSets.Find(req.WorkingSetId);
                        //    var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                        //                    Config.Data.GetKey("output_folder_process"),
                        //                    ws.State,
                        //                    ws.County
                        //                    );

                        //    path = path + @"\" + oldOutputName;
                        //    if (File.Exists(path) && !string.IsNullOrEmpty(oldOutputName))
                        //        System.IO.File.Delete(path);
                        //}


                        watch.Stop();
                        req.TimeCost = Convert.ToInt32(watch.Elapsed.TotalSeconds);
                        req.Status = 2;
                        req.Detail = "";
                        //req.OutputName = outputname;


                        //
                        db.SaveChanges();


                        //Noti
                        var url = Config.Data.GetKey("FA_Site") + "/Noti/" + Config.Data.GetKey("Url_noti_transfer_cols_to_recs")+ "/?reqid=" + req.Id;
                        Console.WriteLine(url);
                        using (var client = new System.Net.WebClient())
                        {
                            var json = client.DownloadString(url);// Config.Get_local_control_site() + "/JSON/TransferColumnsToRecord");
                            Console.WriteLine(json);
                        }
                            
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

                Console.WriteLine("No more request, waiting...");
                System.Threading.Thread.Sleep(10 * 1000);

            }
        }


        public static string download(string state,string county,string filename)
        {
            //var db = new BL.DA_Model();
            
            var rootpath = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                        Config.Data.GetKey("input_folder_process"));
            //var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
            //                            Config.Data.GetKey("input_folder_process"),
            //                            ws.State,
            //                            ws.County
            //                            );
            //path = path + @"\" + wsItem.Filename;
            var remotePath = string.Join("/", new string[] { state, county, filename });
            var localPath = string.Join("\\", new string[] { state, county, filename });
            var path = rootpath + "\\" + localPath;
            if (!File.Exists(path))
            {
                var ftp = new Ftp();
                ftp.download("/" + remotePath, path);
                
            }
            return path;
        }
        public static void upload(string state, string county, string filename, string destinationPath)
        {
            var rootpath = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                        Config.Data.GetKey("input_folder_process"));
            var remotePath = string.Join("/", new string[] { state, county, filename });
            var ftp = new Ftp();
            ftp.upload(destinationPath,"/" + remotePath);
        }
        public static string TransferColumnsToRecord(int wsiId, string strcolumns, string toNewName, string newFileName)
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
                var c = new Dictionary<string, object>(numOfLeftItem);
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
                for (int j = numOfColumns - 1; j >= 0; j--)
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
            var hasSEQ1Field = firstRec.ContainsKey(seq1FieldName.ToString());
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

            var outputPath = Config.Data.GetKey("root_folder_process") + "\\" + Config.Data.GetKey("input_folder_process") + "\\" +
                ws.State + "\\" + ws.County + "\\" + newFileName;
            ReadCSV.Write(outputPath, tmp);
            //dtAll.Clear();
            //dtAll.Dispose();
            tmp = null;
            GC.Collect();
            return outputPath;
            //Helpers.ReadCSV.Write(@"D:\abc.csv", dtAll);


        }
    }
}
