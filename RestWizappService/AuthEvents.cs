using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Microsoft.Net.Http.Headers;
using System.IdentityModel.Tokens.Jwt;

using RestWizappService.Models;

namespace RestWizappService
{
    public class AuthEventsHandler : JwtBearerEvents
    {
        private const string BearerPrefix = "Bearer";

        private AuthEventsHandler() => OnMessageReceived = MessageReceivedHandler;

        /// <summary>
        /// Gets single available instance of <see cref="AuthEventsHandler"/>
        /// </summary>
        public static AuthEventsHandler Instance { get; } = new AuthEventsHandler();

      
        private Task MessageReceivedHandler(MessageReceivedContext context)
        {
            AppConfigModel.tokenType = 0;

            if (context.Request.Headers.TryGetValue("Authorization", out StringValues headerValue))
            {
                string token = headerValue;

                var bearerToken = context.Request.Headers[HeaderNames.Authorization].ToString().Replace("Bearer ", "");

                if (!string.IsNullOrEmpty(token))
                {

                    var handler = new JwtSecurityTokenHandler();
                    var jsonToken = handler.ReadToken(bearerToken);
                    var jwtSecurityToken = jsonToken as JwtSecurityToken;


                    //AppConfigModel.issueTime = jwtSecurityToken.IssuedAt;
                    AppConfigModel.validToTime = jwtSecurityToken.ValidTo.ToString();

                    var claims = jwtSecurityToken.Claims.ToList();

                    int nClaims = claims.Count();

                    string[,] TokenInfo = new string[nClaims, 2];
                    int n = 0;
                    foreach (var claim in claims)
                    {
                        if (claim.Type.ToUpper() == "ROLECODE")
                            AppConfigModel.roleCode = claim.Value;
                        else
                        if (claim.Type.ToUpper() == "USERID")
                            AppConfigModel.userId = claim.Value;
                        else
                        if (claim.Type.ToUpper() == "TOKENTYPE")
                            AppConfigModel.tokenType = Convert.ToInt32(claim.Value);
                        else
                        if (claim.Type.ToUpper() == "APIACCESS")
                            AppConfigModel.apiAccess = claim.Value;
                        else
                        if (claim.Type.ToUpper() == "TOKENGROUPCODE")
                            AppConfigModel.tokenGroupCode = claim.Value;

                    }
                }


                context.Token = bearerToken;
            }

            return Task.CompletedTask;
        }
    }
}
