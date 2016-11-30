namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("FieldOrderAndAction")]
    public partial class FieldOrderAndAction
    {
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Id { get; set; }

        public int WorkingSetItemId { get; set; }

        [StringLength(20)]
        public string OrderType { get; set; }

        public int? DuplicatedAction { get; set; }

        public int? DuplicatedActionType { get; set; }

        [StringLength(5)]
        public string ConcatenateWithDelimiter { get; set; }

        [StringLength(50)]
        public string FieldName { get; set; }
    }
}
