using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RunTransform_column_to_record
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
                if (req == null)
                    req = db.runTransformRequests.FirstOrDefault(p => p.Status == 0 && p.IsReady && !p.IsDeleted);
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


                        var outputname = runProcess(req.WorkingSetId, true);
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
                            if (File.Exists(path) && !string.IsNullOrEmpty(oldOutputName))
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
                System.Threading.Thread.Sleep(10 * 1000);

            }
        }
    }
}
