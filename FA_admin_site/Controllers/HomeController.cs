using System.Web.Mvc;

namespace FA_admin_site.Controllers
{
    [Authorize]
    public class HomeController : Controller
    {
        
        public ActionResult Index()
        {
            UserManager.CreateDefaultUser();
            return View();
        }
    }
}