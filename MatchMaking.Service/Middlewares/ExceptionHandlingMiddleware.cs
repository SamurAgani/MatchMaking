using System.Net;
using MatchMaking.Service.Extensions;

namespace MatchMaking.Service.Middlewares;
public class ExceptionHandlingMiddleware
{
    private readonly RequestDelegate _next;

    private readonly ILogger<ExceptionHandlingMiddleware> _logger;

    public ExceptionHandlingMiddleware(
        RequestDelegate next,
        ILogger<ExceptionHandlingMiddleware> logger)
    {
        _logger = logger;
        _next = next;
    }

    public Task InvokeAsync(HttpContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        return HandleAsync(context);
    }

    private async Task HandleAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception: {InnerExceptions}", ex.GetInnerExceptions());

            context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
        }
    }
}
