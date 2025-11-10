using Microsoft.AspNetCore.Mvc;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Service.Extensions;

namespace MatchMaking.Service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MatchMakingController(IMatchmakingService matchmakingService) : ControllerBase
    {
        [HttpPost("search")]
        public async Task<IActionResult> SearchMatch([FromQuery] string userId, CancellationToken cancellationToken)
        {
            var result = await matchmakingService.SearchMatchAsync(userId, cancellationToken);
            return result.ToActionResult(this);
        }

        [HttpGet("match/{userId}")]
        public async Task<IActionResult> GetMatch(string userId, CancellationToken cancellationToken)
        {
            var result = await matchmakingService.GetMatchAsync(userId, cancellationToken);
            return result.ToActionResult(this);
        }
    }
}
