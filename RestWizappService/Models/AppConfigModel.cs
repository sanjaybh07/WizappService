using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace RestWizappService.Models
{
    public class AppConfigModel
    {
        public static IConfiguration globalConnConfig { get; set; }
       
        public static string DefaultConnectionString { get; set; }
        public static string userId { get; set; } = string.Empty;
        public static string roleCode { get; set; } = string.Empty;
        public static int tokenType { get; set; }
        public static DateTime refreshTokenValidity { get; set; }
        public bool isAuthenticated { get; set; }

        public static  string blobSasToken { get; set; }
        public static string blobServiceEndpoint { get; set; }
        public bool isLoggedIn { get; set; }
        public static bool rereshTokenExpired { get; set; }
        public static string apiRejectedMsg { get; set; } = string.Empty;

        public static string apiFullName { get; set; } = string.Empty;

        public static string apiGroupCode { get; set; }
        public static DateTime issueTime { get; set; } = DateTime.MinValue;
        public static string validToTime { get; set; } = string.Empty;
        public static string apiAccess { get; set; }

        public static string WsConnCOnfig { get; set; }

        public static bool apiNotSecured { get; set; } = false;
        public static string tokenGroupCode { get; set; } = string.Empty;
      
    }



}

