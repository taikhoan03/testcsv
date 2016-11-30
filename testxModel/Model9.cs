namespace testxModel
{
    using System;
    using System.Data.Entity;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Linq;

    public partial class Model9 : DbContext
    {
        public Model9()
            : base("name=Model9")
        {
        }

        public virtual DbSet<FieldRule> FieldRules { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
        }
    }
}
