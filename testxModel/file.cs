namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("file")]
    public partial class file
    {
        public int Id { get; set; }

        [Required]
        [StringLength(50)]
        public string User { get; set; }

        [Required]
        [StringLength(50)]
        public string Name { get; set; }

        public DateTime Create_date { get; set; }

        public bool Is_deleted { get; set; }

        [StringLength(50)]
        public string Status { get; set; }

        public int Packageid { get; set; }

        [Required]
        [StringLength(50)]
        public string State { get; set; }

        [Required]
        [StringLength(50)]
        public string County { get; set; }
    }
}
