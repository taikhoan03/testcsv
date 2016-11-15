namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("MergeFileFromWorkingSet")]
    public partial class MergeFileFromWorkingSet
    {
        public int Id { get; set; }

        public int WorkingSetId { get; set; }

        [Required]
        [StringLength(80)]
        public string Filename { get; set; }

        [StringLength(255)]
        public string Description { get; set; }

        [Required]
        public string JSFile { get; set; }

        [Required]
        public string JSMergeInfos { get; set; }

        [Required]
        public string FileInputNames { get; set; }

        [StringLength(50)]
        public string State { get; set; }

        [StringLength(50)]
        public string County { get; set; }
    }
}
