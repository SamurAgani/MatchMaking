using MatchMaking.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using MatchMaking.Service.Services.Abstracts;

namespace MatchMaking.Service.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MatchMakingController : ControllerBase
    {
        private readonly IKafkaService _kafkaService;
        private readonly IRateLimitService _rateLimitService;
        private readonly IMatchStorageService _matchStorageService;
        private readonly ILogger<MatchMakingController> _logger;

        public MatchMakingController(
            IKafkaService kafkaService,
            IRateLimitService rateLimitService,
            IMatchStorageService matchStorageService,
            ILogger<MatchMakingController> logger)
        {
            _kafkaService = kafkaService;
            _rateLimitService = rateLimitService;
            _matchStorageService = matchStorageService;
            _logger = logger;
        }

        [HttpPost("search")]
        public async Task<IActionResult> SearchMatch([FromQuery] string userId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(userId))
            {
                return BadRequest(new { Error = "UserId is required" });
            }

            try
            {
                var existingMatch = await _matchStorageService.GetMatchForUserAsync(userId, cancellationToken);
                if (existingMatch != null)
                {
                    return BadRequest(new { Error = "You already have an active match", MatchId = existingMatch.MatchId });
                }

                var isInQueue = await _rateLimitService.IsUserInQueueAsync(userId, cancellationToken);
                if (isInQueue)
                {
                    return BadRequest(new { Error = "You are already in the matchmaking queue" });
                }

                var matchRequest = new MatchRequest(userId);
                await _kafkaService.PublishMatchRequestAsync(matchRequest, cancellationToken);

                return NoContent();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process match request for user {UserId}", userId);
                return StatusCode(500, new { Error = "Failed to process match request" });
            }
        }

        [HttpGet("match/{userId}")]
        public async Task<IActionResult> GetMatch(string userId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(userId))
            {
                return BadRequest(new { Error = "UserId is required" });
            }

            try
            {
                var match = await _matchStorageService.GetMatchForUserAsync(userId, cancellationToken);

                if (match == null)
                {
                    return NotFound(new { Error = "No match found for this user" });
                }

                return Ok(new
                {
                    match.MatchId,
                    match.UserIds
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to retrieve match for user {UserId}", userId);
                return StatusCode(500, new { Error = "Failed to retrieve match information" });
            }
        }

        [HttpGet("health")]
        public IActionResult HealthCheck()
        {
            return Ok(new { Status = "Healthy", Service = "MatchMaking Service" });
        }
    }
}
