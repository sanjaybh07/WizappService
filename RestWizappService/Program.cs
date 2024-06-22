using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc.Formatters;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json.Serialization;
using RestWizappService;
using System.Reflection;
using System.Text;
using static IdentityModel.ClaimComparer;
using static System.Net.Mime.MediaTypeNames;

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(
                      policy =>
                      {
                          policy.WithOrigins("http://softinfovm.in",
                                              "http://wizclip.in",
                                              "http://wizapp.in",
                                              "http://localhost:5173", "http://localhost:4200"
                                              )
                                            .AllowAnyHeader()
                                            .AllowAnyMethod(); ;
                      });
});

// Add services to the container.

builder.Services.AddControllers().AddNewtonsoftJson(options =>
options.UseMemberCasing())
.AddJsonOptions(options =>
{
    options.JsonSerializerOptions.WriteIndented = true;
    options.JsonSerializerOptions.Converters.Add(new CustomJsonConverterForType());
});

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme).AddJwtBearer(options =>
{
    options.RequireHttpsMetadata = false;
    options.SaveToken = true;

    builder.Configuration.Bind("JwtSettings", options);
    options.Events = AuthEventsHandler.Instance;

    options.TokenValidationParameters = new TokenValidationParameters()
    {
        ValidateIssuer = true,
        ValidateAudience = true,
        ValidAudience = builder.Configuration["Jwt:Audience"],
        ValidIssuer = builder.Configuration["Jwt:Issuer"],
        IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(builder.Configuration["Jwt:Key"]))
    };
});

builder.Services.AddMvc(options =>
{
    options.AllowEmptyInputInBodyModelBinding = true;
    foreach (var formatter in options.InputFormatters)
    {
        if (formatter.GetType() == typeof(SystemTextJsonInputFormatter))
            ((SystemTextJsonInputFormatter)formatter).SupportedMediaTypes.Add(
            Microsoft.Net.Http.Headers.MediaTypeHeaderValue.Parse("text/plain"));
    }
}).AddJsonOptions(options =>
{
    options.JsonSerializerOptions.PropertyNameCaseInsensitive = true;
    options.JsonSerializerOptions.PropertyNamingPolicy = null;
});


//builder.Services.AddMvcCore(options =>
// {
//     options.RequireHttpsPermanent = true; // does not affect api requests
//     options.RespectBrowserAcceptHeader = true; // false by default
//                                                //options.OutputFormatters.RemoveType<HttpNoContentOutputFormatter>();

// });

//builder.Services.AddScoped<RedirectingAction>();
//builder.Services.AddScoped<ControllerFilterExample>();

// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();

builder.Services.AddSwaggerGen(c =>
{
    var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
    var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
    c.IncludeXmlComments(xmlPath);

    c.AddSecurityDefinition("Bearer", new OpenApiSecurityScheme
    {
        In = ParameterLocation.Header,
        Description = "Please enter a valid token",
        Name = "Authorization",
        Type = SecuritySchemeType.Http,
        BearerFormat = "JWT",
        Scheme = "Bearer"
    });
    c.AddSecurityRequirement(new OpenApiSecurityRequirement
    {
        {
            new OpenApiSecurityScheme
            {
                Reference = new OpenApiReference
                {
                    Type=ReferenceType.SecurityScheme,
                    Id="Bearer"
                }
            },
            new string[]{}
        }
    });


});
//    c =>
//{
//    c.ResolveConflictingActions(apiDescriptions => apiDescriptions.First());
//});

//bool enableSerilog = Convert.ToBoolean(builder.Configuration["Logging:EnableSerilog"]);
//if (enableSerilog)
//{
//    // Configure Serilog
//    Log.Logger = new LoggerConfiguration()
//        .MinimumLevel.Debug()
//        .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
//        .Enrich.FromLogContext()
//        .WriteTo.File("log.txt", rollingInterval: RollingInterval.Day)
//        .CreateLogger();

//    builder.Services.AddLogging(loggingBuilder =>
//    {
//        loggingBuilder.ClearProviders();
//        loggingBuilder.AddSerilog();
//    });
//}

var app = builder.Build();

// Configure the HTTP request pipeline.
//if (app.Environment.IsDevelopment() || app.Environment.IsProduction())


if (true) // app.Environment.IsDevelopment())
    app.UseDeveloperExceptionPage();
else
{
    app.UseExceptionHandler(exceptionHandlerApp =>
    {
        exceptionHandlerApp.Run(async context =>
        {
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;

            // using static System.Net.Mime.MediaTypeNames;
            context.Response.ContentType = Text.Plain;

            await context.Response.WriteAsync("An exception was thrown.");

            var exceptionHandlerPathFeature =
                context.Features.Get<IExceptionHandlerPathFeature>();

            if (exceptionHandlerPathFeature?.Error is FileNotFoundException)
            {
                await context.Response.WriteAsync(" The file was not found.");
            }

            if (exceptionHandlerPathFeature?.Path == "/")
            {
                await context.Response.WriteAsync(" Page: Home.");
            }
        });
    });

    app.UseHsts();
}

app.UseSwagger();
//app.UseSwaggerUI();

app.UseSwaggerUI(config =>
{
    config.ConfigObject.AdditionalItems["syntaxHighlight"] = new Dictionary<string, object>
    {
        ["activated"] = false
    };

});


app.UseDefaultFiles();
app.UseStaticFiles();

app.UseHttpsRedirection();

//app.UseCors(MyAllowSpecificOrigins);

// DO not forget to uncomment above line and comment below line to make API fully secure Whenever our WebApp goes live (Sanjay:13-02-2023) 
app.UseCors(builder =>
{
    builder
    .AllowAnyOrigin()
    .AllowAnyMethod()
    .AllowAnyHeader();
});

app.UseAuthentication();
app.UseAuthorization();

var webSocketOptions = new WebSocketOptions
{
    KeepAliveInterval = TimeSpan.FromMinutes(5)
};


app.UseWebSockets(webSocketOptions);

app.UseMiddleware<myCustomMiddleware>();

app.MapControllers();


app.Run();