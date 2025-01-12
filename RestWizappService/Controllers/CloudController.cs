using Microsoft.AspNetCore.Mvc;
using RestWizappService.Appmethods;
using RestWizappService.Models;
using RestWizappService.Appmethods;
using RestWizappService.Models;
using System.Dynamic;
using System.Net.Http;
using RestWizappService.App_methods;

namespace RestWizappService.Controllers
{
    public class CloudController : ControllerBase
    {


        public commonMethods globalMethods;
        CloudMethods cloudMethod;
        public CloudController(IConfiguration configuration)
        {

            AppConfigModel.globalConnConfig = configuration;
            globalMethods = new commonMethods();
            cloudMethod = new CloudMethods();

            AppConfigModel.blobServiceEndpoint = "https://csg100320029a649e64.blob.core.windows.net/";

            AppConfigModel.blobSasToken = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-09-28T15:42:49Z&st=2024-09-21T07:42:49Z&spr=https,http&sig=xQQBCnC%2FnECo3JDyQ4Rv4syr2hycD%2Fi5qYPkQIjLaaE%3D";
        }

        [HttpGet]
        [Route("~/Containers")]
        public dynamic GetContainers()
        {

            dynamic result = new ExpandoObject();

            string cErr = "";
            result = cloudMethod.GetContainers(ref cErr);

            if (!string.IsNullOrEmpty(cErr))
                return BadRequest(new { Message = cErr });
            else
                return Ok(result);

        }

        [HttpPost]
        [Route("~/RestoreDb")]
        public async Task<IActionResult> RestoreDb(string containerName, string databaseName, string serverName, string restorePath)
        {

            dynamic result = new ExpandoObject();


            if (string.IsNullOrEmpty(containerName))
            {
                containerName = "wizapp5backups";
                databaseName = "stelatoesnew"; //"kartika";
                serverName = "wizapp4.wizapp.in";
                restorePath = "F:\\ClientsData\\ShivaPlus";
            }

            string cErr = "";
            cErr = await cloudMethod.RestoreDbFiles(containerName, databaseName, serverName, restorePath);

            if (!string.IsNullOrEmpty(cErr))
                return BadRequest(new { Message = cErr });
            else
                return Ok(new { Message = "Database restored successfully" });

        }

        [HttpGet]
        [Route("~/VerifyBackkups")]
        public async Task<IActionResult> VerifyBackups(string containerName,string databaseName="")
        {

            if (string.IsNullOrEmpty(containerName))
            {
                containerName = "wizapp5backups";
                databaseName = "STELATOESNEW";
            }

            var(result,cErr) = await cloudMethod.GetVerifiedBackupsList(containerName, databaseName);

            if (!string.IsNullOrEmpty(cErr))
                return BadRequest(new { Message = cErr });
            else
                return Ok(result);

        }

        [HttpGet]
        [Route("~/ListOfDatabases")]
        public dynamic GetAvailableDbs(string containerName)
        {

            dynamic result = new ExpandoObject();

            string cErr = "";
            result = cloudMethod.GetAvailableDbs(containerName, ref cErr);

            if (!string.IsNullOrEmpty(cErr))
                return BadRequest(new { Message = cErr });
            else
                return Ok(result);

        }

    }
}
