using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Helpers;
namespace FA_admin_site.Helpers
{
    public static class helpers
    {
        public static MvcHtmlString SortDirection(this HtmlHelper helper,ref WebGrid grid, string columnName)
        {
            string html = "";
            if (grid.SortColumn == columnName && grid.SortDirection == System.Web.Helpers.SortDirection.Ascending)
                html = "⬆";
            else if (grid.SortColumn == columnName && grid.SortDirection == System.Web.Helpers.SortDirection.Descending)
                html = "⬇";
            else
                html = "";
            return MvcHtmlString.Create(html);
        }
    }
}