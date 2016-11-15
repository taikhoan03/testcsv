using System.Web;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;
using FA_local_site.App_Start;

namespace FA_local_site
{
    public class MvcApplication : HttpApplication
    {
        protected void Application_Start()
        {
            AreaRegistration.RegisterAllAreas();
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);
        }
    }
}