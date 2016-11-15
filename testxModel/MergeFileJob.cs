namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    public partial class MergeFileJob
    {
        public int Id { get; set; }

        [Required]
        [StringLength(2000)]
        public string Filenames { get; set; }

        [Required]
        [StringLength(50)]
        public string State { get; set; }

        [Required]
        [StringLength(50)]
        public string County { get; set; }

        public string MergeDetails { get; set; }

        [StringLength(50)]
        public string Status { get; set; }

        public DateTime? Rundate { get; set; }

        public DateTime? Finishdate { get; set; }

        [StringLength(50)]
        public string Runby { get; set; }

        [StringLength(400)]
        public string ErrorDetail { get; set; }

        public int? WorkingJobId { get; set; }

        [StringLength(80)]
        public string OutputFilename { get; set; }
    }
}
