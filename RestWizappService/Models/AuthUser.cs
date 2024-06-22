using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace RestWizappService.Models
{

    public class objUsers
    {
        public List<User> Users { get; set; }
    }

    public class User
    {
        public string userCode { get; set; }
        public string? userName { get; set; }
        public string passwd { get; set; }
        public string? roleCode { get; set; }
        public Boolean? inactive { get; set; }
        public String? loginId { get; set; }
        public DateTime? refreshTokenValidity { get; set; }
        public string? apiAccess { get; set; }
    }


}





