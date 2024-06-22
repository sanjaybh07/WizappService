using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Filters;
using System.Threading.Tasks;
using RestWizappService.Models;
using Newtonsoft.Json;

namespace RestWizappService
{
    // You may need to install the Microsoft.AspNetCore.Http.Abstractions package into your project
    public class myCustomMiddleware
    {
        private readonly RequestDelegate _next;

        public myCustomMiddleware(RequestDelegate next)
        {
            _next = next;
        }


        private async Task handleAPIErrors(HttpContext httpContext, string cMessage)
        {
            httpContext.Response.StatusCode = 400; // Bad Request
            httpContext.Response.ContentType = "application/json";

            // Create a JSON object with a message key and value
            var message = new { Message = cMessage };

            // Serialize the JSON object and write it to the response body
            var jsonMessage = JsonConvert.SerializeObject(message);
            await httpContext.Response.WriteAsync(jsonMessage);
        }
        public async Task Invoke(HttpContext httpContext)
        {

            AppConfigModel.apiFullName = "";
            var controllerName = httpContext.GetRouteData().Values["controller"];
            var actionName = httpContext.GetRouteData().Values["action"];

            if (controllerName is object && actionName is object)
                AppConfigModel.apiFullName = controllerName.ToString().ToUpper() + "." + actionName.ToString().ToUpper();

                      
            if (AppConfigModel.tokenType == 2 && AppConfigModel.apiFullName != "TOKEN.REISSUEACCESSTOKEN"
                && AppConfigModel.apiFullName != "AUTH.GETUSERS" && AppConfigModel.apiFullName != "AUTH.AUTHENTICATEUSER")
            {

                HttpResponseMessage response = new HttpResponseMessage();

                httpContext.Response.StatusCode = StatusCodes.Status403Forbidden;
                await httpContext.Response.WriteAsync("API call with Refresh token is not allowed...");

                return;
            }

            if (AppConfigModel.tokenType == 1 && AppConfigModel.apiFullName == "TOKEN.REISSUEACCESSTOKEN")
            {

                HttpResponseMessage response = new HttpResponseMessage();

                httpContext.Response.StatusCode = StatusCodes.Status403Forbidden;
                await httpContext.Response.WriteAsync("New Access token cannot be issued with Bearer access token...");

                return;
            }

            await _next(httpContext);
        }
    }



    // Extension method used to add the middleware to the HTTP request pipeline.
    public static class CustomMiddlewareExtensions
    {
        public static IApplicationBuilder UseCustomMiddleware(this IApplicationBuilder builder)
        {
            return builder.UseMiddleware<myCustomMiddleware>();
        }
    }
}
