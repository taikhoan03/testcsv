namespace testxModel
{
    using System;
    using System.Data.Entity;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Linq;

    public partial class Model5 : DbContext
    {
        public Model5()
            : base("name=Model5")
        {
        }

        public virtual DbSet<file> files { get; set; }
        public virtual DbSet<JobFileLayout> JobFileLayouts { get; set; }
        public virtual DbSet<MergeFileFromWorkingSet> MergeFileFromWorkingSets { get; set; }
        public virtual DbSet<MergeFileJob> MergeFileJobs { get; set; }
        public virtual DbSet<package> packages { get; set; }
        public virtual DbSet<WorkingJob> WorkingJobs { get; set; }
        public virtual DbSet<WorkingSet> WorkingSets { get; set; }
        public virtual DbSet<WorkingSetItem> WorkingSetItems { get; set; }

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
