using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using RestWizappService.Appmethods;
using RestWizappService.Models;
using System.Dynamic;

namespace RestWizappService.Controllers
{
    //[Authorize]
    [ApiController]
    [Route("[controller]")]
    public class DataSyncController : ControllerBase
    {
        public DataSyncController(IConfiguration configuration)
        {
            IConfiguration _config = configuration;

            AppConfigModel.globalConnConfig = _config;

        }

        DataSyncMethods syncMethods = new DataSyncMethods();
        /// <summary>
        /// Synch Masters related to POS SKU
        /// </summary>
        /// <param name="cLocId">POS Id</param>
        /// <param name="Body">Sku Para(s) Data</param>
        /// <returns></returns>
        [HttpPost]
        [Route("~/SynchSku")]
        public dynamic SynchSKuPos(string cLocId, [FromBody] PosSkuSynch Body)
        {
            dynamic result = new ExpandoObject();
            string cErr = "";
            
            result = syncMethods.GetSkuDiffData(cLocId, Body,ref cErr);

            if (string.IsNullOrEmpty(cErr))
                return Ok(result);
            else
                return BadRequest(new { Message = cErr });

        }

        /// <summary>
        /// Synch Article attributes
        /// </summary>
        /// <param name="cLocId">POS Id</param>
        /// <param name="Body">Article attributes data</param>
        /// <returns></returns>

        [HttpPost]
        [Route("~/SynchArtAttr")]
        public dynamic SynchArticleAttributes(string cLocId, [FromBody] PosAttrSynch Body)
        {
            dynamic result = new ExpandoObject();
            string cErr = "";
          
            result = syncMethods.GetArticleAttrDiffData(cLocId, Body, ref cErr);

            if (string.IsNullOrEmpty(cErr))
                return Ok(result);
            else
                return BadRequest(new { Message = cErr });
        }

        
    }
}
