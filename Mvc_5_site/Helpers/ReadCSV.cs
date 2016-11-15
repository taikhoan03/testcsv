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

        private static List<IDictionary<string, object>> readFromPath_AsDictionary(string path, decimal limit)
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
                    IgnoreQuotes = true
                };

                using (var csv = new CsvReader(reader, config))
                {
                    var dic = new List<IDictionary<string, object>>();
                    csv.ReadHeader();
                    var fields = csv.FieldHeaders.Select(p => p.Replace(" ", "_")).ToArray();
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
                        dic.Add(myUnderlyingObject);
                    }

                    return dic;



                }
            }
        }
    }
}