using System.Web.Mvc;
namespace Mvc_5_site.Controllers
{
    public class HomeController : Controller
    {
        // GET: Home
        public ActionResult Index()
        {
            
            //var b = (new BL.Class1()).getFiles();
            //var a = Config.Data;
            return View();
        }
        public string Ind()
        {
            return Config.Data.GetKey("root_folder_process");
        }
    }
}