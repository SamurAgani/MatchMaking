using System;

namespace MatchMaking.Service.Extensions;

public static class ExceptionExtension
{
    public static string GetInnerExceptions(this Exception? e)
    {

        if (e == null)
        {
            return string.Empty;
        }

        string messages = string.Empty;
        messages += "\n\n\n\n";
        while (e != null)
        {
            messages += e.Message + " \n";

            e = e.InnerException;
        }
        messages += "\n\n----------------------------------------------------------------------------------------------\n\n";
        return messages;
    }
}
