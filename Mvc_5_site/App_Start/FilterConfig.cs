using Mvc_5_site.Infrastructure;

namespace Mvc_5_site.App_Start
{
    public class FilterConfig
    {
        public static void Configure(System.Web.Mvc.GlobalFilterCollection filters)
        {
            filters.Add(new HandleErrorAttribute());
        }
    }
}
