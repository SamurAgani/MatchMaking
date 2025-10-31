using Microsoft.AspNetCore.Mvc;
using MatchMaking.Service.Services.Abstracts;

namespace MatchMaking.Service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MatchMakingController : ControllerBase
    {
        private readonly IMatchmakingService _matchmakingService;

        public MatchMakingController(IMatchmakingService matchmakingService)
        {
            _matchmakingService = matchmakingService;
        }

        [HttpPost("search")]
        public async Task<IActionResult> SearchMatch([FromQuery] string userId, CancellationToken cancellationToken)
        {
            var result = await _matchmakingService.SearchMatchAsync(userId, cancellationToken);
            return result.ToActionResult(this);
        }

        [HttpGet("match/{userId}")]
        public async Task<IActionResult> GetMatch(string userId, CancellationToken cancellationToken)
        {
            var result = await _matchmakingService.GetMatchAsync(userId, cancellationToken);
            return result.ToActionResult(this);
        }

        [HttpGet("health")]
        public IActionResult HealthCheck()
        {
            return Ok(new { Status = "Healthy", Service = "MatchMaking Service" });
        }
    }
}
