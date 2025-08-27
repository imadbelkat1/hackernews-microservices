# Hacker News Microservices

A distributed microservices implementation of a Hacker News clone built with Go, featuring real-time data synchronization, search capabilities, and scalable architecture.

## Architecture Overview

This project implements a microservices architecture with three core services:

- **News Service**: Fetches data from Hacker News API and publishes to Kafka
- **Indexer Service**: Consumes Kafka messages and stores data in PostgreSQL
- **Search Service**: Consumes Kafka messages, indexes to OpenSearch, and provides search API

## System Architecture

```
Hacker News API → News Service → Kafka → Indexer Service → PostgreSQL
                                    ↓
                                Search Service → OpenSearch
```

### Data Flow

1. **News Service** periodically fetches data from Hacker News API
2. Data is cached in **Redis** and published to **Kafka** topics
3. **Indexer Service** consumes from Kafka and persists to **PostgreSQL**
4. **Search Service** consumes from Kafka and indexes to **OpenSearch**
5. Search API provides full-text search capabilities

## Services

### News Service
- Fetches stories, comments, jobs, asks, polls, and users from Hacker News API
- Implements cron jobs for periodic synchronization
- Caches data in Redis to avoid redundant API calls
- Publishes structured data to Kafka topics

**Key Features:**
- Top stories sync every 1 minute
- Ask stories sync every 1 minute  
- Job stories sync every 1 minute
- Real-time updates sync every 10 seconds
- Redis caching for performance optimization

### Indexer Service
- Consumes data from Kafka topics
- Stores structured data in PostgreSQL
- Handles database migrations automatically
- Provides CRUD operations through repository pattern

**Supported Data Types:**
- Stories with comments and metadata
- User profiles with karma and submissions
- Comments with threading support
- Job postings
- Ask HN posts
- Polls with options

### Search Service (Port 8083)
- Real-time indexing to OpenSearch
- Full-text search API endpoints
- Batch processing for performance
- RESTful search interface

**Search Endpoints:**
- `/api/v1/search/stories` - Search stories
- `/api/v1/search/users` - Search users  
- `/api/v1/search/comments` - Search comments
- `/api/v1/search/jobs` - Search jobs
- `/api/v1/search/asks` - Search ask posts
- `/api/v1/search/polls` - Search polls
- `/api/v1/search/all` - Search across all types

## Technology Stack

- **Language:** Go 1.24.4
- **Message Queue:** Apache Kafka
- **Database:** PostgreSQL 15
- **Cache:** Redis 7
- **Search:** OpenSearch 2.11
- **Containerization:** Docker & Docker Compose

## Prerequisites

- Docker and Docker Compose
- Go 1.24.4+ (for development)
- Git

## Quick Start

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd hackernews-microservices
   ```

2. **Start the infrastructure:**
   ```bash
   docker-compose up -d
   ```

   This starts:
   - PostgreSQL (port 5432)
   - Redis (port 6379)
   - Kafka + Zookeeper (port 9092)
   - OpenSearch cluster (ports 9200, 9600)
   - OpenSearch Dashboards (port 5601)
   - pgAdmin (port 5050)
   - Kafka Control Center (port 9021)

3. **Build and run the services:**

   **News Service:**
   ```bash
   cd news-service
   go mod download
   go run main.go
   ```

   **Indexer Service:**
   ```bash
   cd indexer-service
   go mod download
   go run main.go
   ```

   **Search Service:**
   ```bash
   cd search-service
   go mod download
   go run main.go
   ```

## Configuration

Services use environment variables with sensible defaults:

### Database Configuration
```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=password
DB_NAME=hackernews
DB_SSLMODE=disable
```

### Kafka Configuration
```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=my-client
KAFKA_ACKS=all
```

### Redis Configuration
```env
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
```

### OpenSearch Configuration
```env
OPENSEARCH_URLS=http://localhost:9200
OPENSEARCH_USERNAME=admin
OPENSEARCH_PASSWORD=admin
OPENSEARCH_INDEX=hackernews
```

## Development

### Project Structure
```
├── news-service/          # Data fetching and Kafka publishing
├── indexer-service/       # Kafka consumption and PostgreSQL storage
├── search-service/        # Search indexing and API
├── docker-compose.yml     # Infrastructure setup
└── README.md
```

### Adding New Features

1. **Data Models:** Add new structs in `internal/models/`
2. **API Services:** Implement fetchers in `news-service/internal/services/`
3. **Database:** Add repositories in `indexer-service/internal/repository/`
4. **Search:** Add handlers in `search-service/internal/handlers/`

### Database Migrations

Migrations run automatically on startup. SQL files are located in:
```
indexer-service/pkg/database/migration/create_tables.sql
```

## Monitoring and Management

### Web UIs
- **pgAdmin:** http://localhost:5050 (admin@hn.com / admin123)
- **OpenSearch Dashboards:** http://localhost:5601
- **Kafka Control Center:** http://localhost:9021

### Health Checks
- **Search Service:** http://localhost:8083/health
- **Database:** pgAdmin or direct PostgreSQL connection
- **OpenSearch:** http://localhost:9200/_cluster/health

## API Usage

### Search Examples

**Search Stories:**
```bash
curl "http://localhost:8083/api/v1/search/stories?q=golang&limit=10"
```

**Search Users:**
```bash
curl "http://localhost:8083/api/v1/search/users?q=username"
```

**Search All:**
```bash
curl "http://localhost:8083/api/v1/search/all?q=programming"
```

## Data Synchronization

The system implements several synchronization strategies:

- **Incremental Updates:** Only fetches new/changed items
- **Redis Caching:** Prevents redundant API calls
- **Batch Processing:** Efficient database operations
- **Real-time Streaming:** Kafka enables real-time data flow

## Performance Considerations

- **Concurrent Processing:** Goroutines for parallel API calls
- **Batch Operations:** Bulk inserts and updates
- **Connection Pooling:** Database connection management
- **Caching Strategy:** Redis for frequently accessed data

## Troubleshooting

### Common Issues

1. **Kafka Connection Errors:**
   ```bash
   # Check if Kafka is running
   docker-compose ps broker
   ```

2. **Database Connection Issues:**
   ```bash
   # Verify PostgreSQL is accessible
   docker-compose exec postgres pg_isready -U postgres
   ```

3. **OpenSearch Connection Problems:**
   ```bash
   # Check cluster health
   curl http://localhost:9200/_cluster/health
   ```

### Logs
```bash
# View service logs
docker-compose logs -f postgres
docker-compose logs -f broker
docker-compose logs -f opensearch-node1
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Insert your license information here]

## Acknowledgments

- Hacker News API for providing the data source
- The Go community for excellent libraries and tools
