using System;
using System.Threading.Tasks;
using Microsoft.Owin;
using Owin;
using Microsoft.Owin.Security.Cookies;
using Microsoft.AspNet.Identity;
using Microsoft.Owin.Security;

[assembly: OwinStartup(typeof(WebApiOsp.App_Start.Startup))]

namespace WebApiOsp.App_Start
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            //app.UseCookieAuthentication(new CookieAuthenticationOptions
            //{
            //    AuthenticationType = "ExternalCookie",
            //    AuthenticationMode = AuthenticationMode.Passive,
            //    CookieName = ".AspNet.SomeName",
            //    ExpireTimeSpan = TimeSpan.FromMinutes(5)
            //});
            ConfigureAuthentication(app);
            
            // For more information on how to configure your application, visit http://go.microsoft.com/fwlink/?LinkID=316888
        }
        public void ConfigureAuthentication(IAppBuilder app)
        {
            app.UseCookieAuthentication(new CookieAuthenticationOptions
            {
                AuthenticationType = DefaultAuthenticationTypes.ApplicationCookie,//.ApplicationCookie,
                LoginPath = new PathString("/Login/Index"),
                //AuthenticationMode = AuthenticationMode.Active,
                CookieName = ".AspNet.SomeName",
                ExpireTimeSpan = TimeSpan.FromMinutes(60*1),
                LogoutPath= new PathString("/Login/Logout"),
                SlidingExpiration = true,
                //CookieSecure = CookieSecureOption.Always
            });
            // Use a cookie to temporarily store information about a user logging in with a third party login provider
            app.UseExternalSignInCookie(DefaultAuthenticationTypes.ExternalCookie);

        }
        //public void ConfigureAuth(IAppBuilder app)
        //{
        //    // Enable the application to use a cookie to store information for the signed in user
        //    app.UseCookieAuthentication(new CookieAuthenticationOptions
        //    {
        //        AuthenticationType = DefaultAuthenticationTypes.ApplicationCookie,
        //        LoginPath = new PathString("/Account/LogOn")
        //    });

        //    app.UseExternalSignInCookie(DefaultAuthenticationTypes.ExternalCookie);

        //    // App.Secrets is application specific and holds values in CodePasteKeys.json
        //    // Values are NOT included in repro – auto-created on first load
        //    if (!string.IsNullOrEmpty(App.Secrets.GoogleClientId))
        //    {
        //        app.UseGoogleAuthentication(
        //            clientId: App.Secrets.GoogleClientId,
        //            clientSecret: App.Secrets.GoogleClientSecret);
        //    }

        //    if (!string.IsNullOrEmpty(App.Secrets.TwitterConsumerKey))
        //    {
        //        app.UseTwitterAuthentication(
        //            consumerKey: App.Secrets.TwitterConsumerKey,
        //            consumerSecret: App.Secrets.TwitterConsumerSecret);
        //    }

        //    if (!string.IsNullOrEmpty(App.Secrets.GitHubClientId))
        //    {
        //        app.UseGitHubAuthentication(
        //            clientId: App.Secrets.GitHubClientId,
        //            clientSecret: App.Secrets.GitHubClientSecret);
        //    }

        //    AntiForgeryConfig.UniqueClaimTypeIdentifier = ClaimTypes.NameIdentifier;
        //}







    }
}