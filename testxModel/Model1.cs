namespace testxModel
{
    using System;
    using System.Data.Entity;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Linq;

    public partial class Model1 : DbContext
    {
        public Model1()
            : base("name=Model11")
        {
        }

        public virtual DbSet<file> files { get; set; }
        public virtual DbSet<JobFileLayout> JobFileLayouts { get; set; }
        public virtual DbSet<package> packages { get; set; }
        public virtual DbSet<WorkingJob> WorkingJobs { get; set; }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Entity<package>()
                .Property(e => e.State)
                .IsUnicode(false);

            modelBuilder.Entity<package>()
                .Property(e => e.County)
                .IsUnicode(false);

            modelBuilder.Entity<package>()
                .Property(e => e.Createdby)
                .IsUnicode(false);

            modelBuilder.Entity<package>()
                .Property(e => e.Status)
                .IsUnicode(false);
        }
    }
}
