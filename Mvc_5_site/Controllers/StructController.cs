using System.IO;
using System.Linq;
using System.Web.Mvc;
namespace Mvc_5_site.Controllers
{
    public class StructController : Controller
    {
        // GET: Struct
        public ActionResult Index()
        {
            string path = Path.Combine(Config.Data.GetKey("root_folder_process"),
                                    Config.Data.GetKey("input_folder_process")
                                    );
            //List<string> picFolders = new List<string>();

            //if (Directory.GetDirectories(path, "*.jpg").Length > 0)
            //    picFolders.Add(path);

            //foreach (string dir in Directory.GetDirectories(path, "*", SearchOption.AllDirectories))
            //{
            //    if (Directory.GetFiles(dir, "*.jpg").Length > 0)
            //        picFolders.Add(dir);
            //}

            return View(Directory.GetDirectories(path).Select(p=>(new FileInfo(p)).Name));
            //return View();
        }
        public string Create(string state, string county)
        {
            var path = GetPath(state, county);
            Directory.CreateDirectory(path);
            return string.Empty;
        }

        private static string GetPath(string state, string county)
        {
            return Path.Combine(Config.Data.GetKey(@"root_folder_process"),
                                                Config.Data.GetKey("input_folder_process"),
                                                state,
                                                county
                                                );
        }
    }
}