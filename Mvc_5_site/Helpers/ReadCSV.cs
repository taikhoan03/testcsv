using BL;
using CsvHelper;
using Libs;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Web;

namespace Mvc_5_site.Helpers
{
    public static class ReadCSV
    {
        private static decimal C_Default_limit = 90000000000;
        public static List<IDictionary<string, object>> ReadAsDictionary(string path, decimal limit)
        {
            return readFromPath_AsDictionary(path, limit);
        }
        public static List<ExpandoObject> Read(string path, decimal limit)
        {
            return readFromPath(path, limit);
        }
        public static List<ExpandoObject> Read(string state,string county,string filename, decimal limit)
        {
            var path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process"),
                                    state,
                                    county
                                    );
            path = path + @"\" + filename;
            return readFromPath(path, limit);
        }
        private static List<ExpandoObject> readFromPath(string path, decimal limit)
        {
            using (TextReader reader = System.IO.File.OpenText(path))
            {
                var d = DateTime.Now;
                //string line = reader.ReadLine();
                //Console.WriteLine(line);
                var config = new CsvHelper.Configuration.CsvConfiguration
                {
                    BufferSize = 2048,
                    Delimiter = "\t",
                    IgnoreBlankLines = true,
                    HasHeaderRecord = true,
                    SkipEmptyRecords = true,
                    IgnoreQuotes=true
                };

                using (var csv = new CsvReader(reader, config))
                {
                    var dic = new List<ExpandoObject>();
                    csv.ReadHeader();
                    var fields = csv.FieldHeaders.Select(p=>p.Replace(" ","_")).ToArray();
                    if (limit == 0)
                        limit = C_Default_limit;
                    
                    
                    while (csv.Read() && limit > 0)
                    {
                        limit--;
                        //var key = csv.GetField<string>(0);
                        dynamic MyDynamic = new System.Dynamic.ExpandoObject();
                        IDictionary<string, object> myUnderlyingObject = MyDynamic;
                        for (var i = 0; i < fields.Length; i++)
                        {
                            //var value = csv.GetField(i);
                            //if (value.IsNumeric())
                            //    myUnderlyingObject.Add(fields[i], csv.GetField<decimal>(i));
                            //else
                            myUnderlyingObject.Add(fields[i], csv.GetField(i));
                            //try
                            //{

                            //}
                            //catch (Exception ex)
                            //{

                            //    throw;
                            //}

                        }
                        dic.Add(MyDynamic);
                    }

                    return dic;



                }
            }
        }

        private static List<IDictionary<string, object>> readFromPath_AsDictionary(string path, decimal limit,string delimiter="\t")
        {
            using (TextReader reader = System.IO.File.OpenText(path))
            {
                var d = DateTime.Now;
                //string line = reader.ReadLine();
                //Console.WriteLine(line);
                var config = new CsvHelper.Configuration.CsvConfiguration
                {
                    BufferSize = 2048,
                    Delimiter = delimiter,
                    IgnoreBlankLines = true,
                    HasHeaderRecord = true,
                    SkipEmptyRecords = true,
                    IgnoreQuotes = true
                };

                using (var csv = new CsvReader(reader, config))
                {
                    var dic = new Dictionary<string,IDictionary<string, object>>();
                    csv.ReadHeader();
                    var fields = csv.FieldHeaders.Select(p => p.ReplaceUnusedCharacters()).ToArray();
                    if (limit == 0)
                        limit = C_Default_limit;
                    var line_number = 0;
                    
                    while (csv.Read() && limit > 0)
                    {
                        line_number++;
                        var line = string.Join(delimiter, csv.CurrentRecord);
                        limit--;
                        //var key = csv.GetField<string>(0);
                        dynamic MyDynamic = new System.Dynamic.ExpandoObject();
                        IDictionary<string, object> myUnderlyingObject = MyDynamic;
                        for (var i = 0; i < fields.Length; i++)
                        {
                            //var value = csv.GetField(i);
                            //if (value.IsNumeric())
                            //    myUnderlyingObject.Add(fields[i], csv.GetField<decimal>(i));
                            //else
                            myUnderlyingObject.Add(fields[i], csv.GetField(i));
                            //try
                            //{

                            //}
                            //catch (Exception ex)
                            //{

                            //    throw;
                            //}

                        }
                        myUnderlyingObject.Add("ps___comment", "");

                        if (!dic.ContainsKey(line))
                            dic.Add(line, myUnderlyingObject);
                        else
                        {
                            dic[line]["ps___comment"]+="record at line:"+ line_number+" removed"+Environment.NewLine;
                        }

                    }

                    return dic.Select(p=>p.Value).ToList();



                }
            }
            //using (TextReader reader = System.IO.File.OpenText(path))
            //{
            //    var d = DateTime.Now;
            //    //string line = reader.ReadLine();
            //    //Console.WriteLine(line);
            //    var config = new CsvHelper.Configuration.CsvConfiguration
            //    {
            //        BufferSize = 2048,
            //        Delimiter = "\t",
            //        IgnoreBlankLines = true,
            //        HasHeaderRecord = true,
            //        SkipEmptyRecords = true,
            //        IgnoreQuotes = true
            //    };

            //    using (var csv = new CsvReader(reader, config))
            //    {
            //        var dic = new List<IDictionary<string, object>>();
            //        csv.ReadHeader();
            //        var fields = csv.FieldHeaders.Select(p => p.ReplaceUnusedCharacters()).ToArray();
            //        if (limit == 0)
            //            limit = C_Default_limit;


            //        while (csv.Read() && limit > 0)
            //        {
            //            limit--;
            //            //var key = csv.GetField<string>(0);
            //            dynamic MyDynamic = new System.Dynamic.ExpandoObject();
            //            IDictionary<string, object> myUnderlyingObject = MyDynamic;
            //            for (var i = 0; i < fields.Length; i++)
            //            {
            //                //var value = csv.GetField(i);
            //                //if (value.IsNumeric())
            //                //    myUnderlyingObject.Add(fields[i], csv.GetField<decimal>(i));
            //                //else
            //                    myUnderlyingObject.Add(fields[i], csv.GetField(i));
            //                //try
            //                //{

            //                //}
            //                //catch (Exception ex)
            //                //{

            //                //    throw;
            //                //}

            //            }
            //            dic.Add(myUnderlyingObject);
            //        }

            //        return dic;



            //    }
            //}
        }
        public static void Write(string path, List<IDictionary<string, object>> recs, string delimiter="\t")
        {

            var filename = Path.GetFileName(path);
            var path_ = path.Replace("\\"+filename, "");
            if (!System.IO.Directory.Exists(path_))
            {
                System.IO.Directory.CreateDirectory(path_);
            }


            var header = recs[0].Select(p=>p.Key).ToArray();
            var str_line = string.Join(delimiter, header);
            using (StreamWriter sw = new StreamWriter(path))
            {
                //write header
                sw.WriteLine(str_line);
                sw.Flush();
                //write content
                //while (!sr.EndOfStream)
                //{
                //    string line = sr.ReadLine();
                //    //do some modifications
                //    sw.WriteLine(line);
                //    sw.Flush(); //force line to be written to disk
                //}
                foreach (var rec in recs)
                {
                    var line = rec.Select(p => p.Value).ToArray();
                    str_line = string.Join(delimiter, line);
                    sw.WriteLine(str_line);
                    sw.Flush();
                }

            }
        }
    }
}