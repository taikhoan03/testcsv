namespace testxModel
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("JobFileLayout")]
    public partial class JobFileLayout
    {
        public int Id { get; set; }

        public int WorkingJobId { get; set; }

        [Required]
        [StringLength(50)]
        public string Fieldname { get; set; }

        public int Order { get; set; }

        [Required]
        [StringLength(500)]
        public string Mapper { get; set; }
    }
}
