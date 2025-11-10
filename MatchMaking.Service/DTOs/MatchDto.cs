namespace MatchMaking.Service.DTOs;

public sealed record MatchDto(string MatchId, IReadOnlyList<string> UserIds);
