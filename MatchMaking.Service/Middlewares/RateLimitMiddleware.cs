using MatchMaking.Service.Services.Abstracts;

namespace MatchMaking.Service.Middlewares
{
    public class RateLimitMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly ILogger<RateLimitMiddleware> _logger;

        public RateLimitMiddleware(RequestDelegate next, ILogger<RateLimitMiddleware> logger)
        {
            _next = next;
            _logger = logger;
        }

        public async Task InvokeAsync(HttpContext context, IRateLimitService rateLimitService)
        {
            if (!HttpMethods.IsPost(context.Request.Method))
            {
                await _next(context);
                return;
            }

            if (!context.Request.Query.TryGetValue("userId", out var userIdValues))
            {
                await _next(context);
                return;
            }

            var userId = userIdValues.ToString();
            if (string.IsNullOrWhiteSpace(userId))
            {
                await _next(context);
                return;
            }

            var cancellationToken = context.RequestAborted;
            var isRateLimited = await rateLimitService.IsRateLimitedAsync(userId, cancellationToken);

            if (isRateLimited)
            {
                _logger.LogWarning("Rate limit exceeded for user {UserId}", userId);
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                context.Response.ContentType = "application/json";
                await context.Response.WriteAsJsonAsync(new
                {
                    Error = "Rate limit exceeded. Please wait before making another request."
                });
                return;
            }

            await _next(context);
        }
    }
}
