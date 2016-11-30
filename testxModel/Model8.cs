namespace testxModel
{
    using System;
    using System.Data.Entity;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Linq;

    public partial class Model8 : DbContext
    {
        public Model8()
            : base("name=Model8")
        {
        }

        public virtual DbSet<FieldOrderAndAction> FieldOrderAndActions { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
        }
    }
}
