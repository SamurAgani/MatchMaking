# MatchMaking Service

A scalable, event-driven matchmaking system built with .NET 9.0, using Kafka for event streaming and Redis for queue management and rate limiting.

## 🏗️ Architecture

The system consists of two microservices that communicate via Kafka:

### **MatchMaking.Service** (REST API)

- Accepts matchmaking requests from users
- Enforces rate limiting (100ms minimum interval per user)
- Prevents duplicate queue entries
- Publishes requests to Kafka
- Consumes match completion events
- Stores match results in Redis

### **MatchMaking.Worker** (Background Service)

- Consumes matchmaking requests from Kafka
- Manages player queue in Redis
- Creates matches when enough players are available (configurable, default: 3 players)
- Publishes match completion events to Kafka
- Thread-safe concurrent request handling

### **Infrastructure**

- **Kafka**: Event streaming and message broker
- **Redis**: Queue management, rate limiting, and match storage
- **Zookeeper**: Kafka cluster coordination
- **Kafka UI**: Web interface for monitoring Kafka topics

## 📋 Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [API Documentation](#api-documentation)
- [Configuration](#configuration)
- [Testing](#testing)
- [Architecture Details](#architecture-details)
- [Troubleshooting](#troubleshooting)

## 🔧 Prerequisites

### Required

- [.NET 9.0 SDK](https://dotnet.microsoft.com/download/dotnet/9.0)
- [Docker Desktop](https://www.docker.com/products/docker-desktop) (for infrastructure)

### Optional

- [Visual Studio 2022](https://visualstudio.microsoft.com/) or [VS Code](https://code.visualstudio.com/)
- [Postman](https://www.postman.com/) or similar API testing tool

## 🚀 Quick Start

### Option 1: Run Everything with Docker Compose

```bash
# Clone the repository
git clone <repository-url>
cd MatchMaking

# Start all services (infrastructure + applications)
docker compose up -d

# View logs
docker compose logs -f matchmaking-service
docker compose logs -f matchmaking-worker-1

# Stop all services
docker compose down
```

**Service URLs:**

- MatchMaking API: http://localhost:8080
- Swagger UI: http://localhost:8080/swagger
- Kafka UI: http://localhost:8082
- Redis: localhost:6379
- Kafka: localhost:9092

### Option 2: Run Locally (Recommended for Development)

```bash
# Step 1: Start infrastructure only
docker compose up -d zookeeper kafka redis kafka-ui kafka-init

# Step 2: Build the solution
dotnet build

# Step 3: Run the API service
cd MatchMaking.Service
dotnet run

# Step 4: In a new terminal, run the worker
cd MatchMaking.Worker
dotnet run

# Step 5: Test the API
curl -X POST "http://localhost:5047/api/matchmaking/search?userId=user1"
```

## 📚 API Documentation

### Endpoints

#### 1. **POST** `/api/matchmaking/search`

Add a user to the matchmaking queue.

**Query Parameters:**

- `userId` (required): Unique identifier for the user

**Request:**

```bash
curl -X POST "http://localhost:5047/api/matchmaking/search?userId=user123"
```

**Responses:**

- `204 No Content`: User added to queue successfully
- `400 Bad Request`:
  - User ID is null/empty
  - User has an active match
  - User is already in queue
  - Rate limit exceeded (100ms minimum interval)
- `500 Internal Server Error`: Server error

**Rate Limiting:**

- Minimum interval: 100ms per user
- Enforced at API level with Redis

#### 2. **GET** `/api/matchmaking/match/{userId}`

Retrieve match information for a user.

**Path Parameters:**

- `userId` (required): User identifier

**Request:**

```bash
curl -X GET "http://localhost:5047/api/matchmaking/match/user123"
```

**Responses:**

- `200 OK`: Match found
  ```json
  {
    "matchId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "userIds": ["user1", "user2", "user3"]
  }
  ```
- `404 Not Found`: No match found for user
- `500 Internal Server Error`: Server error

#### 3. **GET** `/api/matchmaking/health`

Health check endpoint.

**Request:**

```bash
curl -X GET "http://localhost:5047/api/matchmaking/health"
```

**Response:**

- `200 OK`: "Healthy"

### Swagger UI

Access interactive API documentation at:

```
http://localhost:5047/swagger
```

## ⚙️ Configuration

### MatchMaking.Service (`appsettings.json`)

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092"
  },
  "Redis": {
    "ConnectionString": "localhost:6379"
  },
  "RateLimit": {
    "MinIntervalMs": 100
  }
}
```

### MatchMaking.Worker (`appsettings.json`)

```json
{
  "PlayersPerMatch": 3,
  "Kafka": {
    "BootstrapServers": "localhost:9092"
  },
  "Redis": {
    "ConnectionString": "localhost:6379"
  }
}
```

## 🧪 Testing

### Run All Tests

```bash
# Run all tests in the solution
dotnet test

# Run tests with detailed output
dotnet test --logger "console;verbosity=detailed"
```

### Run Service Tests

```bash
cd MatchMaking.Service.Test

# Run all tests
dotnet test

# Run only unit tests
dotnet test --filter "FullyQualifiedName~UnitTests"

# Run only integration tests
dotnet test --filter "FullyQualifiedName~IntegrationTests"

# Exclude load tests (require running service)
dotnet test --filter "Category!=LoadTest"
```

### Run Worker Tests

```bash
cd MatchMaking.Worker.Test

# Run all tests
dotnet test

# Run only unit tests
dotnet test --filter "FullyQualifiedName~UnitTests"
```

### Run Load Tests

Load tests require the service to be running:

```bash
# Terminal 1: Start infrastructure
docker compose up -d zookeeper kafka redis kafka-init

# Terminal 2: Start service
cd MatchMaking.Service
dotnet run

# Terminal 3: Run load tests
cd MatchMaking.Service.Test
dotnet test --filter "Category=LoadTest"
```

## 🏛️ Architecture Details

### Components

#### **MatchMaking.Service**

- `MatchMakingController`: REST API endpoints
- `KafkaService`: Producer (matchmaking.request) & Consumer (matchmaking.complete)
- `RedisRateLimitService`: Rate limiting + queue duplicate checking
- `RedisMatchStorageService`: Match persistence (1-hour TTL)

#### **MatchMaking.Worker**

- `Worker`: Background service host
- `KafkaService`: Consumer (matchmaking.request) & Producer (matchmaking.complete)
- `MatchMakingService`: Queue management + match creation logic

#### **MatchMaking.Shared**

- `MatchRequest`: Request model
- `MatchComplete`: Match result model
- `KafkaTopics`: Topic name constants

### Data Flow

1. **User Request**: Client sends POST `/api/matchmaking/search?userId=user1`
2. **Validation**: API checks rate limit, active match, and queue presence
3. **Publish**: API publishes `MatchRequest` to `matchmaking.request` topic
4. **Queue**: Worker adds user to `waiting_players` Redis list
5. **Match Check**: Worker checks if ≥3 players are waiting
6. **Match Creation**: Worker creates match, publishes `MatchComplete` to `matchmaking.complete`
7. **Storage**: API consumes match event, stores in Redis for each user
8. **Retrieval**: Client can GET `/api/matchmaking/match/{userId}` to see match details

### Scalability

- **Multiple Workers**: Run multiple worker instances for horizontal scaling
- **Kafka Partitioning**: Distribute load across multiple partitions
- **Redis Clustering**: Scale Redis for high-throughput scenarios
- **Stateless API**: Scale API service horizontally

## 🔍 Monitoring

### Kafka UI

Access Kafka UI at http://localhost:8082 to:

- View topics and messages
- Monitor consumer groups
- Check broker health
- Inspect message contents

### Logs

#### Service Logs

```bash
# View service logs
tail -f MatchMaking.Service/Logs/matchmaking-service-*.log

# Docker logs
docker compose logs -f matchmaking-service
```

#### Worker Logs

```bash
# View worker logs
tail -f MatchMaking.Worker/Logs/matchmaking-service-*.log

# Docker logs
docker compose logs -f matchmaking-worker-1
```

## 📦 Project Structure

```
MatchMaking/
├── docker-compose.yml              # Infrastructure & services orchestration
├── MatchMaking.sln                 # Solution file
├── MatchMaking.Service/            # REST API Service
│   ├── Controllers/
│   │   └── MatchMakingController.cs
│   ├── Services/
│   │   ├── Abstracts/
│   │   └── Concrete/
│   ├── BackgroundServices/
│   │   └── KafkaBackgroundService.cs
│   └── Program.cs
├── MatchMaking.Worker/             # Background Worker Service
│   ├── Services/
│   │   ├── Abstracts/
│   │   └── Concretes/
│   ├── Worker.cs
│   └── Program.cs
├── MatchMaking.Shared/             # Shared Models & Constants
│   ├── Models/
│   │   ├── MatchRequest.cs
│   │   └── MatchComplete.cs
│   └── Constants/
│       └── KafkaTopics.cs
├── MatchMaking.Service.Test/       # Service Tests
│   ├── UnitTests/
│   ├── IntegrationTests/
│   └── LoadTests/
└── MatchMaking.Worker.Test/        # Worker Tests
    ├── UnitTests/
    └── IntegrationTests/
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 🙏 Acknowledgments

- Built with .NET 9.0
- Uses Confluent.Kafka for event streaming
- Uses StackExchange.Redis for caching
- Powered by Docker for infrastructure

---

**Version:** 1.0.0  
**Last Updated:** October 30, 2025
