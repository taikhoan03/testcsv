﻿using BL;
using CsvHelper;
using Libs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Web;

public static class ReadCSV
{
    private static decimal C_Default_limit = 90000000000;
    public static List<Dictionary<string, object>> ReadAsDictionary(string name, string path, decimal limit)
    {
        name = name.Replace(".", EV.DOT);
        return BL.Ulti.readFromPath_AsDictionary2(name, path, limit);
    }
    public static List<IDictionary<string, object>> ReadAsDictionary(string path, decimal limit)
    {
        return readFromPath_AsDictionary(path, limit);
    }
    public static List<ExpandoObject> Read(string path, decimal limit)
    {
        return readFromPath(path, limit);
    }
    public static List<ExpandoObject> Read(string name, string path, decimal limit)
    {
        name = name.Replace(".", EV.DOT);
        return readFromPath(name, path, limit);
    }
    public static List<ExpandoObject> Read(string state, string county, string filename, decimal limit)
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
                IgnoreQuotes = true
            };

            using (var csv = new CsvReader(reader, config))
            {
                var dic = new List<ExpandoObject>();
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
                    dic.Add(MyDynamic);
                }

                return dic;



            }
        }
    }
    private static List<ExpandoObject> readFromPath(string name, string path, decimal limit)
    {
        using (TextReader reader = System.IO.File.OpenText(path))
        {
            var d = DateTime.Now;
            //string line = reader.ReadLine();
            //Console.WriteLine(line);
            var config = new CsvHelper.Configuration.CsvConfiguration
            {
                BufferSize = 4096,
                Delimiter = "\t",
                IgnoreBlankLines = true,
                HasHeaderRecord = true,
                SkipEmptyRecords = true,
                IgnoreQuotes = true
            };

            using (var csv = new CsvReader(reader, config))
            {
                var ls = new List<ExpandoObject>();
                csv.ReadHeader();
                var fields = csv.FieldHeaders.Select(p => p.ReplaceUnusedCharacters()).ToArray();
                //var fields = header_str.Split(new string[] { delimiter }, StringSplitOptions.None)
                //.Select(p => p.ReplaceUnusedCharacters()).ToArray();
                if (limit == 0)
                    limit = C_Default_limit;
                var numOfField = fields.Length;

                while (csv.Read() && limit > 0)
                {
                    limit--;
                    //var key = csv.GetField<string>(0);
                    dynamic MyDynamic = new System.Dynamic.ExpandoObject();
                    IDictionary<string, object> myUnderlyingObject = MyDynamic;
                    for (var i = 0; i < numOfField; i++)
                    {
                        //var value = csv.GetField(i);
                        //if (value.IsNumeric())
                        //    myUnderlyingObject.Add(fields[i], csv.GetField<decimal>(i));
                        //else
                        //myUnderlyingObject.Add(fields[i], csv.GetField(i));
                        myUnderlyingObject.Add(name + EV.DOLLAR + fields[i], csv.GetField(i));
                        //try
                        //{

                        //}
                        //catch (Exception ex)
                        //{

                        //    throw;
                        //}

                    }
                    ls.Add(MyDynamic);
                }

                return ls;



            }
        }
    }
    //private static List<IDictionary<string, object>> readFromPath_AsDictionary(string path, decimal limit,string delimiter="\t")
    //{


    //    using (TextReader reader = System.IO.File.OpenText(path))
    //    {
    //        var d = DateTime.Now;
    //        //string line = reader.ReadLine();
    //        //Console.WriteLine(line);
    //        var config = new CsvHelper.Configuration.CsvConfiguration
    //        {
    //            BufferSize = 2048,
    //            Delimiter = delimiter,
    //            IgnoreBlankLines = true,
    //            HasHeaderRecord = true,
    //            SkipEmptyRecords = true,
    //            IgnoreQuotes = true
    //        };

    //        using (var csv = new CsvReader(reader, config))
    //        {
    //            var dic = new Dictionary<string,IDictionary<string, object>>();
    //            //var dic=new HashSet<
    //            csv.ReadHeader();
    //            var fields = csv.FieldHeaders.Select(p => p.ReplaceUnusedCharacters()).ToArray();
    //            if (limit == 0)
    //                limit = C_Default_limit;
    //            var line_number = 0;

    //            while (csv.Read() && limit > 0)
    //            {
    //                line_number++;
    //                var line = string.Join(delimiter, csv.CurrentRecord);
    //                limit--;
    //                //var key = csv.GetField<string>(0);
    //                dynamic MyDynamic = new System.Dynamic.ExpandoObject();
    //                IDictionary<string, object> myUnderlyingObject = MyDynamic;
    //                for (var i = 0; i < fields.Length; i++)
    //                {
    //                    myUnderlyingObject.Add(fields[i], csv.GetField(i));
    //                }
    //                myUnderlyingObject.Add("ps___comment", "");

    //                if (!dic.ContainsKey(line))
    //                    dic.Add(line, myUnderlyingObject);
    //                else
    //                {
    //                    dic[line]["ps___comment"] += "record at line:" + line_number + " removed[[]]";// +Environment.NewLine;
    //                }

    //            }

    //            return dic.Select(p=>p.Value).ToList();



    //        }
    //    }

    //}
    private static List<IDictionary<string, object>> readFromPath_AsDictionary(string path, decimal limit, string delimiter = "\t")
    {
        return null;

        //using (TextReader reader = System.IO.File.OpenText(path))
        //{
        //    var d = DateTime.Now;
        //    //string line = reader.ReadLine();
        //    //Console.WriteLine(line);
        //    var header_str = reader.ReadLine();
        //    var fields = header_str.Split(new string[] { delimiter},StringSplitOptions.None)
        //        .Select(p => p.ReplaceUnusedCharacters()).ToArray();


        //    string line;
        //    var dic = new HashSet<CustomData>();
        //    while ((line = reader.ReadLine()) != null && limit > 0)
        //    {
        //        limit--;
        //        var lineArr= line.Split(new string[] { delimiter }, StringSplitOptions.None);
        //        dynamic MyDynamic = new System.Dynamic.ExpandoObject();
        //        IDictionary<string, object> myUnderlyingObject = MyDynamic;
        //        for (var i=0;i<fields.Length;i++)
        //        {
        //            myUnderlyingObject.Add(fields[i], lineArr[i]);


        //        }
        //        var data = new CustomData()
        //        {
        //            Data = myUnderlyingObject,
        //            pureTextData = line,
        //        };
        //        dic.Add(data);
        //    }
        //    return dic.Select(p => p.Data).ToList();



        //}

    }
    private static List<IDictionary<string, object>> readFromPath_AsDictionary(string name, string path, decimal limit, string delimiter = "\t")
    {
        var w = System.Diagnostics.Stopwatch.StartNew();
        var header_str = "";
        int numOfRow = 0;
        var hashedLine = new HashSet<string>();
        var seperator = new string[] { delimiter };
        //var header_str = reader.ReadLine();
        var slipOption = StringSplitOptions.None;
        using (StreamReader sr = File.OpenText(path))
        {
            var header = sr.ReadLine();
            header_str = header;
            while (!sr.EndOfStream && limit > 0)
            {
                limit--;

                var line = sr.ReadLine();
                hashedLine.Add(line);
                numOfRow += 1;

            }
        } //Finished. Close the file


        var rename = name + EV.DOLLAR;
        var fields = header_str.Split(new string[] { delimiter }, slipOption)
            .Select(p => p.ReplaceUnusedCharacters()).ToArray();
        var numOfField = fields.Length;
        var arrNewFieldName = fields.Select(p => rename + p).ToArray();
        var rs = new List<IDictionary<string, object>>(hashedLine.Count);

        foreach (var line in hashedLine)
        {
            var MyDynamic = new ExpandoObject();
            IDictionary<string, object> myUnderlyingObject = MyDynamic;
            var linearr = line.Split(seperator, slipOption);
            for (var j = 0; j < numOfField; j++)
            {
                myUnderlyingObject.Add(arrNewFieldName[j], linearr[j]);
            }
            rs.Add(myUnderlyingObject);
        }

        hashedLine.Clear();
        hashedLine = null;
        w.Stop();
        Console.WriteLine("Elapsed TotalSeconds " + w.Elapsed.TotalSeconds + ", numOfRec:" + rs.Count);
        return rs;

        // code2
        //var w = System.Diagnostics.Stopwatch.StartNew();
        //var hashedLine = new List<string[]>();
        //var numOfField = 0;
        //var arrNewFieldName = new string[1];
        //var rs = new List<IDictionary<string, object>>(hashedLine.Count);
        //using (TextReader reader = System.IO.File.OpenText(path))
        //{
        //    var header_str = reader.ReadLine();
        //    var slipOption = StringSplitOptions.None;
        //    var fields = header_str.Split(new string[] { delimiter }, slipOption)
        //        .Select(p => p.ReplaceUnusedCharacters()).ToArray();

        //    numOfField = fields.Length;

        //    var lineArr = new string[numOfField];
        //    string line;
        //    //var hs_line = new HashSet<string>();
        //    var rename = name + EV.DOLLAR;
        //    arrNewFieldName = fields.Select(p => rename + p).ToArray();

        //    var seperator = new string[] { delimiter };


        //    while (!string.IsNullOrWhiteSpace(line = reader.ReadLine()) && limit > 0)
        //    {
        //        limit--;

        //        hashedLine.Add(line.Split(seperator, slipOption));
        //    }

        //    rs = new List<IDictionary<string, object>>(hashedLine.Count);



        //}
        //foreach (var ln in hashedLine)
        //{
        //    var MyDynamic = new ExpandoObject();
        //    IDictionary<string, object> myUnderlyingObject = MyDynamic;
        //    for (var i = 0; i < numOfField; i++)
        //    {
        //        myUnderlyingObject.Add(arrNewFieldName[i], ln[i]);
        //    }
        //    rs.Add(myUnderlyingObject);
        //}
        //hashedLine.Clear();
        //hashedLine = null;
        //Console.WriteLine("Elapsed TotalSeconds " + w.Elapsed.TotalSeconds + ", numOfRec:" + rs.Count);
        //return rs.ToList();



    }
    private static List<Dictionary<string, object>> readFromPath_AsDictionary2(string name, string path, decimal limit, string delimiter = "\t")
    {
        var w = System.Diagnostics.Stopwatch.StartNew();
        var header_str = "";
        int numOfRow = 0;
        var hashedLine = new HashSet<string>();
        var seperator = new string[] { delimiter };
        //var header_str = reader.ReadLine();
        var slipOption = StringSplitOptions.None;
        using (StreamReader sr = File.OpenText(path))
        {
            var header = sr.ReadLine();
            header_str = header;
            while (!sr.EndOfStream && limit > 0)
            {
                limit--;

                var line = sr.ReadLine();
                hashedLine.Add(line);
                numOfRow += 1;

            }
        } //Finished. Close the file


        var rename = name + EV.DOLLAR;
        var fields = header_str.Split(new string[] { delimiter }, slipOption)
            .Select(p => p.ReplaceUnusedCharacters()).ToArray();
        var numOfField = fields.Length;
        var arrNewFieldName = fields.Select(p => rename + p).ToArray();
        var rs = new List<Dictionary<string, object>>(hashedLine.Count);

        foreach (var line in hashedLine)
        {
            //var MyDynamic = new ExpandoObject();
            Dictionary<string, object> myUnderlyingObject = new Dictionary<string, object>(numOfField);
            var linearr = line.Split(seperator, slipOption);
            for (var j = 0; j < numOfField; j++)
            {
                myUnderlyingObject.Add(arrNewFieldName[j], linearr[j]);
            }
            rs.Add(myUnderlyingObject);
        }

        hashedLine.Clear();
        hashedLine = null;
        w.Stop();
        Console.WriteLine("Elapsed TotalSeconds " + w.Elapsed.TotalSeconds + ", numOfRec:" + rs.Count);
        return rs;




    }
    public static void Write(string path, List<IDictionary<string, object>> recs, string delimiter = "\t")
    {

        var filename = Path.GetFileName(path);
        var path_ = path.Replace("\\" + filename, "");
        if (!System.IO.Directory.Exists(path_))
        {
            System.IO.Directory.CreateDirectory(path_);
        }


        var header = recs[0].Select(p => p.Key).ToArray();
        var str_line = string.Join(delimiter, header);
        using (StreamWriter sw = new StreamWriter(path))
        {
            //write header
            try
            {
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
            catch (Exception)
            {

                throw;
            }
            finally
            {
                sw.Close();
                sw.Dispose();
            }

        }
    }
    public static void Write(string path, DataTable table, string delimiter = "\t")
    {

        var filename = Path.GetFileName(path);
        var path_ = path.Replace("\\" + filename, "");
        if (!System.IO.Directory.Exists(path_))
        {
            System.IO.Directory.CreateDirectory(path_);
        }

        var lsHeader = new List<string>();
        foreach (DataColumn column in table.Columns)
        {
            lsHeader.Add(column.ColumnName);
        }
        var header = lsHeader.ToArray();// recs[0].Select(p => p.Key).ToArray();
        var str_line = string.Join(delimiter, header);
        using (StreamWriter sw = new StreamWriter(path))
        {
            //write header
            try
            {
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
                foreach (DataRow rec in table.Rows)
                {
                    var line = rec.ItemArray;//.Select(p => p.Value).ToArray();
                    str_line = string.Join(delimiter, line);
                    sw.WriteLine(str_line);
                    sw.Flush();
                }
            }
            catch (Exception)
            {

                throw;
            }
            finally
            {
                sw.Close();
                sw.Dispose();
            }

        }
    }
    public class CustomData//: IEqualityComparer<CustomData>
    {
        public IDictionary<string, object> Data { get; set; }
        public string pureTextData { get; set; }

        //public override bool Equals(object x)
        //{
        //    return true;
        //    //return pureTextData.GetHashCode()== ((CustomData)x).pureTextData.GetHashCode();
        //    //return this.pureTextData == ((CustomData)x).pureTextData;// StringComparer.InvariantCultureIgnoreCase
        //    //.Equals(this.pureTextData, ((CustomData)x).pureTextData);
        //}
        //public override int GetHashCode()
        //{
        //    //var a=this.pureTextData.GetHashCode();
        //    return base.GetHashCode();// StringComparer.InvariantCultureIgnoreCase.GetHashCode(this.pureTextData);
        //}
        //public int GetHashCode(CustomData obj)
        //{
        //    //throw new NotImplementedException();
        //    return StringComparer.InvariantCultureIgnoreCase
        //                 .GetHashCode(obj);
        //}
    }
}