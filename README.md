# KafkaRetryDLQNet

A minimal .NET 10 demonstration of Kafka retry topics with three retry stages and a Dead Letter Queue (DLQ), using pure Kafka headers and a wait-before-consume strategy.

## Overview

This project demonstrates a robust message processing pattern with automatic retry logic and error handling:

- **Main Topic**: Initial message processing
- **Retry Topics (3 stages)**: Progressive retry with different delay strategies
  - `retry-1`: Fixed delay (5 seconds)
  - `retry-2`: Linear delay (15 seconds)
  - `retry-3`: Exponential with jitter (30-40 seconds)
- **Dead Letter Queue**: Final destination for messages that fail all retry attempts

### Key Features

- **Pure Kafka Headers**: Message routing metadata stored in Kafka headers
- **Wait-Before-Consume**: Consumers wait if messages arrive early (no republish loop)
- **Single Process**: All workers run in one host application
- **SQL Integration**: Demonstrates real-world scenario with Northwind database updates

## Architecture

### Kafka Headers

Messages are routed using these Kafka headers:

- `x-retry-stage` (int): Current retry stage (0-3)
- `x-not-before-epoch-ms` (long): Timestamp when message should be processed
- `x-origin-topic` (string): Original topic the message came from
- `x-last-error` (string): Last error message encountered

### Components

1. **MessageProducer**: Produces employee update messages to the main topic every 10 seconds
2. **MainConsumer**: Processes main topic; routes failures to retry-1
3. **Retry1Consumer**: Processes retry-1 topic; routes failures to retry-2
4. **Retry2Consumer**: Processes retry-2 topic; routes failures to retry-3
5. **Retry3Consumer**: Processes retry-3 topic; routes failures to DLQ
6. **EmployeeRepository**: Updates Northwind database Employee records
7. **MessageRouter**: Handles routing logic with delay calculations

### Message Flow

```
main → MainConsumer → (on failure) → retry-1 (wait 5s)
                                         ↓
                              Retry1Consumer → (on failure) → retry-2 (wait 15s)
                                                                  ↓
                                                      Retry2Consumer → (on failure) → retry-3 (wait 30-40s)
                                                                                         ↓
                                                                              Retry3Consumer → (on failure) → deadletter
```

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download)
- [Docker](https://www.docker.com/get-started) and Docker Compose

## Getting Started

### 1. Start Infrastructure

Start Kafka, Zookeeper, and SQL Server using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- Kafka on `localhost:9092`
- Zookeeper on `localhost:2181`
- SQL Server on `localhost:1433` with Northwind database

Wait about 30 seconds for SQL Server to initialize the database.

### 2. Build and Run the Application

```bash
dotnet build
dotnet run --project KafkaRetryDLQNet/KafkaRetryDLQNet.csproj
```

### 3. Observe the Logs

The application will:
1. Create all necessary Kafka topics
2. Start producing employee update messages
3. Process messages through the retry pipeline

You'll see logs showing:
- Messages being produced
- Successful processing
- Retry routing with delays
- Wait times before processing early arrivals

## Configuration

Configuration is in `appsettings.json`:

```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "Topics": {
      "Main": "main",
      "Retry1": "retry-1",
      "Retry2": "retry-2",
      "Retry3": "retry-3",
      "DeadLetter": "deadletter"
    },
    "RetryDelays": {
      "Retry1Ms": 5000,
      "Retry2Ms": 15000,
      "Retry3BaseMs": 30000,
      "Retry3JitterMs": 10000
    },
    "ProducerIntervalMs": 10000
  },
  "ConnectionStrings": {
    "Northwind": "Server=localhost,1433;Database=Northwind;User Id=sa;Password=YourStrong@Passw0rd;TrustServerCertificate=True;"
  }
}
```

## Testing the Retry Logic

The application processes messages successfully by default. To test retry logic, you can:

1. Stop the SQL Server container temporarily to trigger failures:
   ```bash
   docker-compose stop sqlserver
   ```

2. Messages will fail and route through retry topics:
   - First failure → retry-1 (waits 5 seconds)
   - Second failure → retry-2 (waits 15 seconds)
   - Third failure → retry-3 (waits 30-40 seconds)
   - Fourth failure → deadletter

3. Restart SQL Server to allow messages to succeed:
   ```bash
   docker-compose start sqlserver
   ```

## Project Structure

```
KafkaRetryDLQNet/
├── docker-compose.yml          # Infrastructure setup
├── init-db.sql                 # Northwind database initialization
├── KafkaRetryDLQNet.sln        # Solution file
└── KafkaRetryDLQNet/           # Main project
    ├── KafkaRetryDLQNet.csproj
    ├── appsettings.json
    ├── Program.cs              # Application entry point
    ├── KafkaSettings.cs        # Configuration models
    ├── EmployeeMessage.cs      # Message model
    ├── HeaderHelper.cs         # Kafka header utilities
    ├── TopicCreator.cs         # Topic initialization
    ├── MessageProducer.cs      # Message producer
    ├── MessageRouter.cs        # Retry routing logic
    ├── EmployeeRepository.cs   # SQL repository
    ├── MainConsumer.cs         # Main topic consumer
    ├── Retry1Consumer.cs       # Retry-1 consumer
    ├── Retry2Consumer.cs       # Retry-2 consumer
    └── Retry3Consumer.cs       # Retry-3 consumer
```

## Cleanup

Stop and remove all containers:

```bash
docker-compose down -v
```

## License

This is a demonstration project for educational purposes.
