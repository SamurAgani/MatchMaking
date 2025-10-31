using FluentResults;
using Microsoft.AspNetCore.Mvc;

namespace MatchMaking.Service
{
    public static class FluentResultsHttpExtensions
    {
        public static IActionResult ToActionResult(this Result result, ControllerBase controller)
        {
            if (result.IsSuccess) return controller.NoContent();
            return controller.ProblemFrom(result);
        }

        public static IActionResult ToActionResult<T>(this Result<T> result, ControllerBase controller)
        {
            if (result.IsSuccess) return controller.Ok(result.Value);
            return controller.ProblemFrom(result.ToResult());
        }

        private static IActionResult ProblemFrom(this ControllerBase controller, Result result)
        {
            var status = result.Errors
                .Select(e => e.Metadata.TryGetValue("status", out var s) ? s : null)
                .OfType<int>()
                .FirstOrDefault();

            if (status == 0) status = 400;

            var title = result.Errors.FirstOrDefault()?.Message ?? "Request failed";
            var detail = string.Join(" | ", result.Errors.Select(e => e.Message).Distinct());

            return controller.Problem(statusCode: status, title: title, detail: detail);
        }
    }
}
