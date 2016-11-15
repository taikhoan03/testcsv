namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("package")]
    public partial class package
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
        public string Createdby { get; set; }

        public DateTime Createddate { get; set; }

        [StringLength(50)]
        public string Status { get; set; }

        public bool? Isdeleted { get; set; }
    }
}
