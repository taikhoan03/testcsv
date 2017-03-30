using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using BL;
namespace FA_admin_site.Controllers
{
    public class NotiController : Controller
    {
        // GET: Noti
        public ActionResult Index()
        {
            return View();
        }
        public string Transfer_cols_to_records(int reqid)
        {
            var newfileid = 0;
            using (var db_ = new DA_Model())
            {
                using (var dbcontexttransaction = db_.Database.BeginTransaction())
                {
                    var req = db_.req_Transfer_Columns_to_Records.Find(reqid);
                    
                    try
                    {
                        var found = db_.workingSetItems.FirstOrDefault(p => p.WorkingSetId == req.WorkingSetId && req.OutputName == p.Filename);
                        var wsi = db_.workingSetItems.Find(req.WorkingSetItemId);
                        var ws = db_.workingSets.Find(req.WorkingSetId);
                        var file = db_.files.FirstOrDefault(p => p.State == ws.State && p.County == ws.County && p.Name == wsi.Filename);
                        if (found == null)
                        {
                            
                            var db_newwsitem = new BL.WorkingSetItem();
                            db_newwsitem.Filename = req.OutputName;
                            db_newwsitem.IsLayouted = false;
                            db_newwsitem.IsMerged = false;
                            db_newwsitem.PrimaryKey = wsi.PrimaryKey;
                            db_newwsitem.WorkingSetId = wsi.WorkingSetId;
                            db_.workingSetItems.Add(db_newwsitem);
                            db_.SaveChanges();
                            newfileid = db_newwsitem.Id;

                            

                            var f = new BL.file();
                            f.County = ws.County;
                            f.Create_date = DateTime.Now;
                            f.Is_deleted = false;
                            f.Name = req.OutputName;
                            f.Packageid = file.Packageid;
                            f.State = ws.State;
                            f.User = file.User;// System.Web.HttpContext.Current.User.Identity.Name;
                            f.Status = "Processing";
                            db_.files.Add(f);
                            db_.SaveChanges();
                            dbcontexttransaction.Commit();



                        }
                        if (newfileid == 0)
                            newfileid = file.Id;

                        var wsController = new WorkingSetController();
                        wsController.GetLayout(newfileid);



                    }
                    catch (Exception ext)
                    {
                        dbcontexttransaction.Rollback();

                        throw new Exception("some reason to rethrow", ext);
                    }
                    

                }
                
            }
            
            return "fine";
        }
        public string Transfer_Tax_Installment(int reqid)
        {
            var newfileid = 0;
            using (var db_ = new DA_Model())
            {
                using (var dbcontexttransaction = db_.Database.BeginTransaction())
                {
                    var req = db_.req_Transfer_NumOfTaxInstallments.Find(reqid);

                    try
                    {
                        var found = db_.workingSetItems.FirstOrDefault(p => p.WorkingSetId == req.WorkingSetId && req.OutputName == p.Filename);
                        var wsi = db_.workingSetItems.Find(req.WorkingSetItemId);
                        var ws = db_.workingSets.Find(req.WorkingSetId);
                        var file = db_.files.FirstOrDefault(p => p.State == ws.State && p.County == ws.County && p.Name == wsi.Filename);
                        if (found == null)
                        {

                            var db_newwsitem = new BL.WorkingSetItem();
                            db_newwsitem.Filename = req.OutputName;
                            db_newwsitem.IsLayouted = false;
                            db_newwsitem.IsMerged = false;
                            db_newwsitem.PrimaryKey = wsi.PrimaryKey;
                            db_newwsitem.WorkingSetId = wsi.WorkingSetId;
                            db_.workingSetItems.Add(db_newwsitem);
                            db_.SaveChanges();
                            newfileid = db_newwsitem.Id;



                            var f = new BL.file();
                            f.County = ws.County;
                            f.Create_date = DateTime.Now;
                            f.Is_deleted = false;
                            f.Name = req.OutputName;
                            f.Packageid = file.Packageid;
                            f.State = ws.State;
                            f.User = file.User;// System.Web.HttpContext.Current.User.Identity.Name;
                            f.Status = "Processing";
                            db_.files.Add(f);
                            db_.SaveChanges();
                            dbcontexttransaction.Commit();



                        }
                        if (newfileid == 0)
                            newfileid = file.Id;

                        var wsController = new WorkingSetController();
                        wsController.GetLayout(newfileid);



                    }
                    catch (Exception ext)
                    {
                        dbcontexttransaction.Rollback();

                        throw new Exception("some reason to rethrow", ext);
                    }


                }

            }

            return "fine";
        }
    }
}