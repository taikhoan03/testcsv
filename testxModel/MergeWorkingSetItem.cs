namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("MergeWorkingSetItem")]
    public partial class MergeWorkingSetItem
    {
        public int Id { get; set; }

        public int MergeWorkingSetId { get; set; }

        [StringLength(50)]
        public string Filename { get; set; }

        [StringLength(100)]
        public string PrimaryKey { get; set; }

        public string SecondaryKeys { get; set; }
    }
}
