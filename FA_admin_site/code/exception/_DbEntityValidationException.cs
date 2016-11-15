using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace FA_admin_site.code.exception
{
    public class _DbEntityValidationException: System.Data.Entity.Validation.DbEntityValidationException
    {
        public string msg_result;
        public _DbEntityValidationException()
        {
        }
        public _DbEntityValidationException(string message)
        : base(message)
        {
            msg_result = string.Empty;
            foreach (var eve in this.EntityValidationErrors)
            {
                msg_result+=string.Format("Entity of type \"{0}\" in state \"{1}\" has the following validation errors:",
                    eve.Entry.Entity.GetType().Name, eve.Entry.State);
                foreach (var ve in eve.ValidationErrors)
                {
                    msg_result += string.Format("- Property: \"{0}\", Error: \"{1}\"",
                        ve.PropertyName, ve.ErrorMessage);
                }
            }
        }

        public _DbEntityValidationException(string message, Exception inner)
        : base(message, inner)
        {
            msg_result = string.Empty;
            foreach (var eve in this.EntityValidationErrors)
            {
                msg_result += string.Format("Entity of type \"{0}\" in state \"{1}\" has the following validation errors:",
                    eve.Entry.Entity.GetType().Name, eve.Entry.State);
                foreach (var ve in eve.ValidationErrors)
                {
                    msg_result += string.Format("- Property: \"{0}\", Error: \"{1}\"",
                        ve.PropertyName, ve.ErrorMessage);
                }
            }
        }
    }
}