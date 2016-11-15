namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("WorkingSetItem")]
    public partial class WorkingSetItem
    {
        public int Id { get; set; }

        public int WorkingSetId { get; set; }

        [StringLength(50)]
        public string Filename { get; set; }

        [StringLength(100)]
        public string PrimaryKey { get; set; }

        public string SecondaryKeys { get; set; }

        public bool? IsMerged { get; set; }
    }
}
