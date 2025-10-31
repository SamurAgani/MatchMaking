using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchMaking.Shared.Constants
{
    public static class KafkaTopics
    {
        public const string MatchmakingRequest = "matchmaking.request";
        public const string MatchmakingComplete = "matchmaking.complete";
    }
}
