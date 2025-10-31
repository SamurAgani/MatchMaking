using FluentAssertions;
using NBomber.CSharp;
using NBomber.Http;
using NBomber.Http.CSharp;

namespace MatchMaking.Service.Test.LoadTests;

public class MatchMakingLoadTests
{
    private const string BaseUrl = "http://localhost:5047";

    [Fact]
    [Trait("Category", "LoadTest")]
    public void LoadTest_SearchMatch_100ConcurrentUsers()
    {
        using var httpClient = new HttpClient();

        var scenario = Scenario.Create("search_match_load_test", async context =>
        {
            var userId = $"load-user-{context.ScenarioInfo.InstanceId}-{Guid.NewGuid()}";
            var url = $"{BaseUrl}/api/matchmaking/search?userId={userId}";

            var request = Http.CreateRequest("POST", url);

            var response = await Http.Send(httpClient, request);

            return response;
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            Simulation.Inject(rate: 50, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(30))
        );

        var stats = NBomberRunner
            .RegisterScenarios(scenario)
            .Run();

        var scen = stats.ScenarioStats[0];
        scen.Ok.Request.Count.Should().BeGreaterThan(0, "some requests should succeed");
        scen.Ok.Request.RPS.Should().BeGreaterThan(0, "should have positive RPS");
        scen.Ok.Latency.Percent99.Should().BeLessThan(1000, "99th percentile latency should be under 1 second");
    }

    [Fact]
    [Trait("Category", "LoadTest")]
    public void LoadTest_GetMatch_HighThroughput()
    {
        using var httpClient = new HttpClient();

        var scenario = Scenario.Create("get_match_load_test", async context =>
        {
            var instanceNum = int.TryParse(context.ScenarioInfo.InstanceId, out var id) ? id : 0;
            var userId = $"user-{instanceNum % 100}";
            var url = $"{BaseUrl}/api/matchmaking/match/{userId}";

            var request = Http.CreateRequest("GET", url);

            var response = await Http.Send(httpClient, request);

            return response;
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            Simulation.Inject(rate: 100, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(20))
        );

        var stats = NBomberRunner
            .RegisterScenarios(scenario)
            .Run();

        var scen = stats.ScenarioStats[0];

        var totalRequests = scen.Ok.Request.Count + scen.Fail.Request.Count;
        totalRequests.Should().BeGreaterThan(1000, "should handle high request volume");

        var totalRPS = scen.Ok.Request.RPS + scen.Fail.Request.RPS;
        totalRPS.Should().BeGreaterThan(50, "should handle high read throughput");

        if (scen.Ok.Request.Count > 0)
        {
            scen.Ok.Latency.Percent50.Should().BeLessThan(500, "median latency should be under 500ms");
        }
        if (scen.Fail.Request.Count > 0)
        {
            scen.Fail.Latency.Percent50.Should().BeLessThan(500, "median latency for 404 should be under 500ms");
        }
    }

    [Fact]
    [Trait("Category", "LoadTest")]
    public void LoadTest_RateLimit_VerifyEnforcement()
    {
        using var httpClient = new HttpClient();
        var singleUserId = "rate-limit-test-user";

        var scenario = Scenario.Create("rate_limit_test", async context =>
        {
            var url = $"{BaseUrl}/api/matchmaking/search?userId={singleUserId}";

            var request = Http.CreateRequest("POST", url);

            var response = await Http.Send(httpClient, request);

            return response;
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(1))
        .WithLoadSimulations(
            Simulation.Inject(rate: 100, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(5))
        );

        var stats = NBomberRunner
            .RegisterScenarios(scenario)
            .Run();

        var scen = stats.ScenarioStats[0];
        scen.Fail.Request.Count.Should().BeGreaterThan(0, "rate limiting should cause some requests to fail");
        var badRequestStatusCodes = scen.Fail.StatusCodes.Where(sc => sc.StatusCode == "400").ToList();
        badRequestStatusCodes.Should().NotBeEmpty("rate limited requests should return 400");
    }

    [Fact]
    [Trait("Category", "LoadTest")]
    public void LoadTest_SustainedLoad_StabilityTest()
    {
        using var httpClient = new HttpClient();

        var scenario = Scenario.Create("sustained_load_test", async context =>
        {
            var userId = $"sustained-user-{Guid.NewGuid()}";
            var url = $"{BaseUrl}/api/matchmaking/search?userId={userId}";

            var request = Http.CreateRequest("POST", url);

            var response = await Http.Send(httpClient, request);

            return response;
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            Simulation.KeepConstant(copies: 50, during: TimeSpan.FromMinutes(1))
        );

        var stats = NBomberRunner
            .RegisterScenarios(scenario)
            .Run();

        var scen = stats.ScenarioStats[0];
        scen.Ok.Request.Count.Should().BeGreaterThan(1000, "should handle sustained load");
        scen.Fail.Request.Count.Should().BeLessThan(scen.Ok.Request.Count / 10, "failure rate should be low");
        scen.Ok.Latency.Percent95.Should().BeLessThan(500, "95th percentile should remain stable");
    }

    [Fact]
    [Trait("Category", "LoadTest")]
    public void LoadTest_MixedWorkload_RealisticScenario()
    {
        using var httpClient = new HttpClient();

        var searchScenario = Scenario.Create("search_scenario", async context =>
        {
            var userId = $"search-user-{Guid.NewGuid()}";
            var url = $"{BaseUrl}/api/matchmaking/search?userId={userId}";

            var request = Http.CreateRequest("POST", url);
            return await Http.Send(httpClient, request);
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            Simulation.Inject(rate: 30, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(30))
        );

        var getMatchScenario = Scenario.Create("get_match_scenario", async context =>
        {
            var instanceNum = int.TryParse(context.ScenarioInfo.InstanceId, out var id) ? id : 0;
            var userId = $"user-{instanceNum % 50}";
            var url = $"{BaseUrl}/api/matchmaking/match/{userId}";

            var request = Http.CreateRequest("GET", url);
            return await Http.Send(httpClient, request);
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            Simulation.Inject(rate: 50, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(30))
        );

        var stats = NBomberRunner
            .RegisterScenarios(searchScenario, getMatchScenario)
            .Run();

        stats.ScenarioStats.Should().HaveCount(2);

        var searchStats = stats.ScenarioStats.First(s => s.ScenarioName == "search_scenario");
        searchStats.Ok.Request.Count.Should().BeGreaterThan(0, "search requests should succeed");

        var getMatchStats = stats.ScenarioStats.First(s => s.ScenarioName == "get_match_scenario");
        var totalGetRequests = getMatchStats.Ok.Request.Count + getMatchStats.Fail.Request.Count;
        totalGetRequests.Should().BeGreaterThan(0, "get match scenario should send requests");

        searchStats.Ok.Latency.Percent99.Should().BeLessThan(1000, "search latency should be acceptable");
    }

    [Fact]
    [Trait("Category", "LoadTest")]
    public void StressTest_FindBreakingPoint()
    {
        using var httpClient = new HttpClient();

        var scenario = Scenario.Create("stress_test", async context =>
        {
            var userId = $"stress-user-{Guid.NewGuid()}";
            var url = $"{BaseUrl}/api/matchmaking/search?userId={userId}";

            var request = Http.CreateRequest("POST", url);
            return await Http.Send(httpClient, request);
        })
        .WithWarmUpDuration(TimeSpan.FromSeconds(5))
        .WithLoadSimulations(
            Simulation.RampingInject(rate: 10, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(30)),
            Simulation.RampingInject(rate: 50, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(30)),
            Simulation.RampingInject(rate: 100, interval: TimeSpan.FromSeconds(1), during: TimeSpan.FromSeconds(30))
        );

        var stats = NBomberRunner
            .RegisterScenarios(scenario)
            .Run();

        var scen = stats.ScenarioStats[0];
        scen.Ok.Request.Count.Should().BeGreaterThan(0);
    }
}
