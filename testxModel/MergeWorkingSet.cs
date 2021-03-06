namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("MergeWorkingSet")]
    public partial class MergeWorkingSet
    {
        public int Id { get; set; }

        [Required]
        [StringLength(50)]
        public string State { get; set; }

        [Required]
        [StringLength(50)]
        public string County { get; set; }

        public int Edition { get; set; }

        public int Version { get; set; }

        [Required]
        [StringLength(50)]
        public string User { get; set; }

        public DateTime? Createdate { get; set; }

        [StringLength(50)]
        public string Status { get; set; }
    }
}
