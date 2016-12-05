using Microsoft.AspNet.Identity;
using Microsoft.Owin.Security;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Web;
using System.Web.Mvc;
using Microsoft.Owin.Host.SystemWeb;
using Owin;

namespace FA_admin_site.Controllers
{
    public class LoginController : Controller
    {
        [AllowAnonymous]
        // GET: Login
        public ActionResult Index()
        {
            
            ViewBag.username = "";
            return View();
        }
        [AllowAnonymous]
        public void Init()
        {
            UserManager.CreateDefaultUser();
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Index(string username, string password)
        {
            if (new UserManager().IsValid(username, password))
            {
                var claims = new List<Claim>();
                // Setting    
                claims.Add(new Claim(ClaimTypes.Name, username));
                var claimIdenties = new ClaimsIdentity(claims, DefaultAuthenticationTypes.ApplicationCookie);
                var ctx = Request.GetOwinContext();
                var authenticationManager = ctx.Authentication;

                HttpContext.GetOwinContext().Authentication.SignIn(
                   new AuthenticationProperties {
                       //AllowRefresh = true,
                       IsPersistent = false,
                       ExpiresUtc = DateTime.UtcNow.AddDays(7)
                   }, claimIdenties);
                return RedirectToAction("Index","Home"); // auth succeed 
            }
            // invalid username or password
            ModelState.AddModelError("", "invalid username or password");
            ViewBag.username = username;
            return View();
        }
        [HttpGet]
        public ActionResult Logout()
        {
            IdentitySignout();
            if (ControllerContext.HttpContext.Request.Cookies[".AspNet.SomeName"] != null)
            {
                HttpCookie myCookie = new HttpCookie(".AspNet.SomeName");
                myCookie.Expires = DateTime.Now.AddYears(-1);
                ControllerContext.HttpContext.Response.Cookies.Add(myCookie);
            }
            //System.Security.Authentication.SignOut(DefaultAuthenticationTypes.ApplicationCookie);
            return RedirectToAction("Index");
        }
        public void IdentitySignout()
        {
            HttpContext.GetOwinContext().Authentication.SignOut(DefaultAuthenticationTypes.ApplicationCookie,
                                            DefaultAuthenticationTypes.ExternalCookie);
        }
    }
    class UserManager
    {
        public bool IsValid(string username, string password)
        {
            using (var db = new BL.DA_Model()) // use your DbConext
            {
                var pwd = BL.WebHelper.EncodePasswordMd5(password).Replace("-", "");
                // if your users set name is Users
                return db.users.Any(u => u.Username == username
                    && u.Pwd == pwd);
            }
        }
        public static void CreateDefaultUser()
        {
            using (var db = new BL.DA_Model()) // use your DbConext
            {
                var ad = new BL.User()
                {
                    Username = "admin",
                    IsActive = true,
                    LastLogin = DateTime.Now,
                    Pwd = BL.WebHelper.EncodePasswordMd5("admin1").Replace("-", ""),
                    Role = 0
                };
                var user1 = new BL.User()
                {
                    Username = "user1",
                    IsActive = true,
                    LastLogin = DateTime.Now,
                    Pwd = BL.WebHelper.EncodePasswordMd5("user1").Replace("-", ""),
                    Role = 0
                };
                var user2 = new BL.User()
                {
                    Username = "user2",
                    IsActive = true,
                    LastLogin = DateTime.Now,
                    Pwd = BL.WebHelper.EncodePasswordMd5("user2").Replace("-", ""),
                    Role = 0
                };
                if (db.users.Count() == 0)
                {
                    db.users.Add(ad);
                    db.users.Add(user1);
                    db.users.Add(user2);
                    db.SaveChanges();
                }
            }
        }
    }
    
}