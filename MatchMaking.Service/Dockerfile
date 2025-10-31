FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080
EXPOSE 8081


FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["MatchMaking.Service/MatchMaking.Service.csproj", "MatchMaking.Service/"]
COPY ["MatchMaking.Shared/MatchMaking.Shared.csproj", "MatchMaking.Shared/"]
RUN dotnet restore "./MatchMaking.Service/MatchMaking.Service.csproj"
COPY . .
WORKDIR "/src/MatchMaking.Service"
RUN dotnet build "./MatchMaking.Service.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./MatchMaking.Service.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MatchMaking.Service.dll"]