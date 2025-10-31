using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchMaking.Shared.Models
{
    public record MatchComplete(string MatchId, List<string> UserIds);
}
