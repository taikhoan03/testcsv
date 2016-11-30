using System;
using System.Diagnostics;
using System.Web.Mvc;

namespace Mvc_5_site.Infrastructure
{
    /// <summary>
    /// Apply this attribute to controller actions to log the exception via Trace.
    /// </summary>
    /// <remarks>
    /// If ExceptionHandled is true in context then no action will be taken.
    /// Marks ExceptionHandled to true.
    /// </remarks>
    [AttributeUsage(
        AttributeTargets.Class | AttributeTargets.Method,
        AllowMultiple = true,
        Inherited = true)]
    public class HandleErrorAttribute : System.Web.Mvc.HandleErrorAttribute
    {
        public override void OnException(ExceptionContext filterContext)
        {
            if (!filterContext.ExceptionHandled)
            {
                if (filterContext.Exception != null)
                {
                    Trace.TraceError(filterContext.Exception.ToString());
                }
                filterContext.ExceptionHandled = true;

                filterContext.HttpContext.Response.StatusCode = 500;
                filterContext.Result = new JsonResult
                {
                    Data = new
                    {
                        // obviously here you could include whatever information you want about the exception
                        // for example if you have some custom exceptions you could test
                        // the type of the actual exception and extract additional data
                        // For the sake of simplicity let's suppose that we want to
                        // send only the exception message to the client
                        errorMessage = filterContext.Exception.Message
                    },
                    JsonRequestBehavior = JsonRequestBehavior.AllowGet
                };
            }
        }
    }
}