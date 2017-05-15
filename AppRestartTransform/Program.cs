using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppRestartTransform
{
    class Program
    {
        static FileSystemWatcher watcher;
        static void Main(string[] args)
        {
            if (File.Exists(@"D:\FA_in_out\restart.flag"))
            {
                doJob();
            }else
            {
                watch();
            }
            Console.WriteLine(@"Monitoring D:\FA_in_out\restart.flag");
            Console.Read();
        }
        
        private static void watch()
        {
            watcher = new FileSystemWatcher();
            watcher.Path = @"D:\FA_in_out";
            watcher.NotifyFilter = NotifyFilters.Attributes |
    NotifyFilters.CreationTime |
    NotifyFilters.FileName |
    NotifyFilters.LastAccess |
    NotifyFilters.LastWrite |
    NotifyFilters.Size |
    NotifyFilters.Security;
            watcher.Filter = "restart.flag";
            watcher.Created += new FileSystemEventHandler(OnChanged);
            watcher.Changed += new FileSystemEventHandler(OnChanged);
            watcher.Renamed += new RenamedEventHandler(OnChanged);
            watcher.EnableRaisingEvents = true;
        }

        

        private static void OnChanged(object sender, FileSystemEventArgs e)
        {
            try
            {
                doJob();
                //process.WaitForExit();// Waits here for the process to exit.
            }
            catch (Exception)
            {
                
            }
            //throw new NotImplementedException();
        }
        private static void doJob()
        {
            File.Delete(@"D:\FA_in_out\restart.flag");
            Process[] p = Process.GetProcesses();// ("RunTransform");
            foreach (var item in p)
            {
                if(item.MainWindowTitle== "RunTransform")
                    item.Kill();
            }
            Process process = new Process();
            // Configure the process using the StartInfo properties.
            process.StartInfo.FileName = @"D:\FA\RunTransformApp\AppRunTransform.exe";
            //process.StartInfo.Arguments = "-n";
            process.StartInfo.WindowStyle = ProcessWindowStyle.Normal;
            process.Start();
        }
    }
}
