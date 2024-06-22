using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;
using System.IdentityModel.Tokens.Jwt;
using TasksApi.Helpers;
using System.Dynamic;
using System.Net;
using RestWizappService.Models;
using RestWizappService.Appmethods;

namespace AspNetCoreApp.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class AuthController : ControllerBase
    {
        public IConfiguration _configuration;
        String connStr;

        commonMethods globalMethods = new commonMethods();
        public AuthController(IConfiguration config)
        {
            _configuration = config;
            AppConfigModel.globalConnConfig = _configuration;
        }

        [HttpGet(Name = "test")]
        public string Index()
        {
            return "sucess!";
        }


        [HttpGet]
        [Route("~/~/Users")]
        public object Getusers(string GroupCode)
        {
            String cErr = "";

            commonMethods globalMethods = new commonMethods();

            AppConfigModel.apiGroupCode= GroupCode;
            SqlConnection conn = globalMethods.GetSqlConnection(ref cErr);

            if (!string.IsNullOrEmpty(cErr))
            {
                return BadRequest(cErr);
            }

            dynamic result = new ExpandoObject();


            commonMethods userMethod = new commonMethods();


            result = userMethod.GetUsersList(conn);

            return this.Ok(result);

        }


        [HttpPost]
        [Route("~/validateUser")]
        public async Task<IActionResult> AuthenticateUser(string GroupCode, User _userData)
        {
            if (_userData != null && _userData.userName != null && _userData.passwd != null)
            {

                string cErr = "";
                AppConfigModel.apiGroupCode= GroupCode;
                SqlConnection conn = globalMethods.GetSqlConnection(ref cErr);

                if (!string.IsNullOrEmpty(cErr))
                {
                    return BadRequest(cErr);
                }


                User user = GetUser(conn, _userData.userName, _userData.passwd);

                if (user != null && user.roleCode != null)
                {

                    _userData.roleCode = user.roleCode;
                    _userData.apiAccess = user.apiAccess;
                    _userData.userName = user.userName;
                    _userData.userCode = user.userCode;

                    var accessTokenData = tokenHelper.GenerateToken(_configuration, _userData, 1);

                    var refreshTokenData = tokenHelper.GenerateToken(_configuration, _userData, 2);

                    dynamic tokenObj = new ExpandoObject();

                    tokenObj.accessToken = new JwtSecurityTokenHandler().WriteToken(accessTokenData);
                    tokenObj.refreshToken = new JwtSecurityTokenHandler().WriteToken(refreshTokenData);

                    string cMessage = updateRefreshTokenValidity(conn, _userData.userCode);

                    if (String.IsNullOrEmpty(cMessage))
                        return Ok(tokenObj);
                    else
                        return BadRequest(cMessage);

                    //return Ok(new JwtSecurityTokenHandler().WriteToken(token));
                }
                else
                {
                    return BadRequest("Invalid credentials");
                }
            }
            else
            {
                return BadRequest();
            }
        }


        protected string updateRefreshTokenValidity(SqlConnection conn, string userId)
        {


            try
            {
                string cExpr = $"update users SET refreshTokenValidity='{AppConfigModel.refreshTokenValidity}'" +
                $" where user_Code='{userId}'";

                SqlCommand cmd = new SqlCommand(cExpr, conn);

                conn.Open();
                cmd.ExecuteNonQuery();
                conn.Close();

                return "";
            }
            catch (Exception ex)
            {
                return ex.Message.ToString();
            }
        }

        [NonAction]
        //private async Task<UserInfo> GetUser(string userCode, string password)
        protected User GetUser(SqlConnection conn, string userName, string password)
        {
            User userInfo = new User();



            string cExpr = $"Select top 1 user_Code userCode,passwd,role_id roleCode,(CASE WHEN isnull(api_access,0)=1 THEN '1' ELSE '' END) apiaccess" +
                $" from users (nolock) where userName='{userName}'";


            SqlCommand cmd = new SqlCommand(cExpr, conn);
            DataTable dtExists = new DataTable();
            SqlDataAdapter sda = new SqlDataAdapter(cmd);

            sda.Fill(dtExists);



            if (dtExists.Rows.Count > 0)
            {
                string storedPwd = globalMethods.Encrypt(dtExists.Rows[0]["passwd"].ToString());

                if (storedPwd.Trim().ToUpper() == password.Trim().ToUpper())
                {
                    userInfo.userCode = dtExists.Rows[0]["userCode"].ToString();
                    userInfo.roleCode = dtExists.Rows[0]["roleCode"].ToString();
                    userInfo.apiAccess = dtExists.Rows[0]["apiAccess"].ToString();
                }
            }

            //return await userInfo.FirstOrDefaultAsync(u => u.userId == userCode && u.Password == password);

            return userInfo;
        }

    }
}

