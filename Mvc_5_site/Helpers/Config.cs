using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using ReadConfig;
using System.IO;
namespace Mvc_5_site
{
    public class Config
    {
        public static Helper Data = new Helper(HttpRuntime.AppDomainAppPath+@"\Config");
        static Config()
        {
            // perform initialization here
            //Create root folder if not exits
            var root_folder = Data.GetKey("root_folder_process");
            if(!Directory.Exists(root_folder))
                Directory.CreateDirectory(root_folder);

            var input_folder_name= Data.GetKey("input_folder_process");
            var input_folder = Path.Combine(root_folder, input_folder_name);
            if (!Directory.Exists(input_folder))
                Directory.CreateDirectory(input_folder);
        }
    }
}