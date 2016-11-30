namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("FieldRule")]
    public partial class FieldRule
    {
        public int Id { get; set; }

        public int WorkingSetItemId { get; set; }

        [Required]
        [StringLength(50)]
        public string Name { get; set; }

        public string ExpValue { get; set; }

        public int? Order { get; set; }

        public int? Type { get; set; }
    }
}
