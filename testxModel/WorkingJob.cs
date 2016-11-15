namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("WorkingJob")]
    public partial class WorkingJob
    {
        public int Id { get; set; }

        [Required]
        [StringLength(50)]
        public string User { get; set; }

        [Required]
        [StringLength(50)]
        public string State { get; set; }

        [Required]
        [StringLength(50)]
        public string County { get; set; }

        [Required]
        [StringLength(50)]
        public string Edtion { get; set; }

        [Required]
        [StringLength(50)]
        public string Version { get; set; }

        [Required]
        [StringLength(50)]
        public string Filename { get; set; }

        public DateTime Createdate { get; set; }

        [StringLength(100)]
        public string PrimaryKey { get; set; }

        public string SecondaryKey { get; set; }
    }
}
