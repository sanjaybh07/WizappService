using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using Microsoft.AspNetCore.Authorization;
using TasksApi.Helpers;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using Microsoft.AspNetCore.Diagnostics;
using System.Runtime.ConstrainedExecution;
using RestWizappService.Appmethods;
using RestWizappService.Models;

namespace RestWizappService.Controllers
{
    [Authorize]
    [ApiController]
    [Route("[controller]")]
    public class TokenController : ControllerBase
    {

        IConfiguration _config;
        String connStr;

        public TokenController(IConfiguration configuration)
        {
            _config = configuration;

            AppConfigModel.globalConnConfig = _config;
            connStr = configuration["ConnectionStrings:CON_DEFAULT"];

            AppConfigModel.DefaultConnectionString = connStr;
        }

        [HttpGet]
        [Route("~/getAccessToken")]
        public IActionResult reIssueAccessToken()
        {
            String cErr = "";

            SqlConnection conn = new commonMethods().GetSqlConnection(ref cErr);

            if (!string.IsNullOrEmpty(cErr))
                return BadRequest(new { Message = cErr });



            dynamic result = new ExpandoObject();
            dynamic retResult = new ExpandoObject();

            result.Message = "";

            tokenHelper helperMethod = new tokenHelper();

            retResult = helperMethod.validateRefreshToken(conn);
            if (string.IsNullOrEmpty(retResult.Message))
            {
                User _userdata;

                _userdata = new User();
                _userdata.userCode = AppConfigModel.userId;
                _userdata.roleCode = AppConfigModel.roleCode;

                var accessTokenData = tokenHelper.GenerateToken(_config, _userdata, 1);

                result.accessToken = new JwtSecurityTokenHandler().WriteToken(accessTokenData);
            }
            else
            {
                result.Message = retResult.Message;
                result.tokenExpired = retResult.tokenExpired;
            }

            return Ok(result);

        }

    }
}
