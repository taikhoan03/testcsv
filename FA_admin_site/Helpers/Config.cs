using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using ReadConfig;
using System.IO;
public class Config
{
    public static Helper Data = new Helper(Path.Combine( AppDomain.CurrentDomain.BaseDirectory , nameof(Config)));
    static Config()
    {
        // perform initialization here
        //Create root folder if not exits
        //var root_folder = Data.GetKey("root_folder_process");
        //Directory.CreateDirectory(root_folder);

        //var input_folder_name= Data.GetKey("input_folder_process");
        //var input_folder = Path.Combine(root_folder, input_folder_name);
        //Directory.CreateDirectory(input_folder);
        //local_control_site = Data.GetKey("local_control_sile");
    }
    public static string Get_local_control_site()
    {
        return Data.GetKey("local_control_site");
    }
}