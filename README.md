# NL2API - Natural Language to API

**Converting SQL queries into API calls with parallel execution and intelligent caching**

```
Converting natural language into API calls is an interesting and challenging task in Natural Language Processing (NLP) and Machine Learning. This project typically involves several key steps:

Understanding and Parsing the Natural Language Request: Recognizing the user's intent and relevant information.
Mapping the Intent to API Operations: Translating the recognized intent into actual API calls.
Constructing the API Request: Building the correct API request based on the parsed data.
Executing the API Call and Returning the Result: Interacting with the backend system via APIs and returning the result.
```

## 🚀 Quick Start

**NL2API** is a sophisticated service that bridges the gap between SQL queries and REST APIs. It allows users to query data using familiar SQL syntax while the system automatically translates these queries into API calls, executes them in parallel, and merges the results.

### Key Benefits
- **Familiar Interface**: Use SQL syntax to query APIs
- **Parallel Execution**: Automatically executes multiple API calls in parallel
- **Result Merging**: Handles complex JOIN operations between different APIs
- **Performance Optimization**: Built-in caching system for frequently accessed data
- **Error Handling**: Comprehensive error management and logging

### Prerequisites
- Docker and Docker Compose installed
- Basic understanding of SQL and REST APIs
- Access to the target APIs you want to query

### Quick Start
1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd nl2api
   ```

2. **Start the services**:
   ```bash
   cd sql2api-agent
   docker-compose up -d
   ```

3. **Verify the service is running**:
   ```bash
   curl http://localhost:8000/health
   ```

4. **Test with a simple query**:
   ```bash
   curl -X POST http://localhost:8000/execute \
     -H "Content-Type: application/json" \
     -d '{
       "reportId": 1,
       "sql": "SELECT * FROM users LIMIT 5",
       "question": "Get first 5 users"
     }'
   ```

## 📋 Table of Contents
1. [System Overview](#system-overview)
2. [Configuration](#configuration)
3. [API Usage](#api-usage)
4. [SQL Query Support](#sql-query-support)
5. [API Mapping Configuration](#api-mapping-configuration)
6. [Caching System](#caching-system)
7. [Error Handling](#error-handling)
8. [Deployment](#deployment)
9. [Troubleshooting](#troubleshooting)

## System Overview

### Architecture
The system consists of several key components:

1. **SQL Parser**: Analyzes SQL queries to extract tables, conditions, and operations
2. **API Mapper**: Maps database tables to REST API endpoints
3. **API Executor**: Performs asynchronous API calls
4. **Result Merger**: Combines results from multiple API calls
5. **Cache Manager**: Manages Redis-based caching
6. **Configuration Manager**: Handles system configuration

### Technology Stack
- **Backend**: Python 3.11 with FastAPI
- **Database**: MySQL 8.0
- **Caching**: Redis
- **Container**: Docker with Docker Compose
- **SQL Parsing**: sqlparse library

## Configuration

### Environment Variables
Create a `.env` file in the `sql2api-agent` directory:

```env
# Database Configuration
DB_HOST=mysql
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=nl2api_db

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=

# Application Configuration
APP_HOST=0.0.0.0
APP_PORT=8000
LOG_LEVEL=INFO
CACHE_TTL=300
MAX_CONCURRENT_REQUESTS=10
```

### Configuration File
The main configuration is in `sql2api-agent/config/config.yaml`:

```yaml
app:
  name: "NL2API Service"
  version: "1.0.0"
  debug: false

database:
  pool_size: 10
  max_overflow: 20

cache:
  ttl: 300  # seconds
  max_connections: 50

api:
  timeout: 30  # seconds
  retry_attempts: 3
  retry_delay: 1  # seconds
```

## API Usage

### Main Endpoint
**POST** `/execute`

### Request Format
```json
{
  "reportId": 123,
  "sql": "SELECT id, name FROM users WHERE status = 'active'",
  "question": "Get active users"
}
```

**Parameters:**
- `reportId` (required): Unique identifier for the query
- `sql` (required): SQL query string
- `question` (optional): Natural language description of the query

### Response Format
```json
{
  "status": 0,
  "message": "success",
  "data": [
    {
      "id": 1,
      "name": "John Doe"
    },
    {
      "id": 2,
      "name": "Jane Smith"
    }
  ],
  "cache_hit": false,
  "execution_time": 0.245
}
```

**Response Fields:**
- `status`: 0 for success, non-zero for errors
- `message`: Status message
- `data`: Query results
- `cache_hit`: Whether result was served from cache
- `execution_time`: Time taken to execute (seconds)

### Error Response
```json
{
  "status": 400,
  "message": "Invalid SQL syntax",
  "data": null,
  "cache_hit": false
}
```

## SQL Query Support

### Supported SQL Features

#### Basic Queries
```sql
SELECT * FROM users
SELECT id, name, email FROM users
SELECT DISTINCT department FROM employees
```

#### WHERE Conditions
```sql
SELECT * FROM users WHERE id = 1
SELECT * FROM products WHERE price > 100
SELECT * FROM users WHERE name LIKE '%John%'
SELECT * FROM orders WHERE status IN ('pending', 'shipped')
```

#### Joins
```sql
-- Inner Join
SELECT u.name, o.total
FROM users u
INNER JOIN orders o ON u.id = o.user_id

-- Multiple joins
SELECT u.name, o.total, p.product_name
FROM users u
INNER JOIN orders o ON u.id = o.user_id
INNER JOIN products p ON o.product_id = p.id
```

#### Sorting and Limiting
```sql
SELECT * FROM users ORDER BY created_at DESC
SELECT * FROM products ORDER BY price ASC LIMIT 10
SELECT * FROM orders ORDER BY total DESC LIMIT 5 OFFSET 10
```

#### Aggregations (Limited Support)
```sql
SELECT COUNT(*) FROM users
SELECT MAX(price) FROM products
SELECT MIN(age) FROM users
```

### SQL Limitations
- No support for subqueries
- No support for GROUP BY or HAVING clauses
- No support for UNION operations
- Limited aggregation functions
- No support for database functions (DATE(), UPPER(), etc.)

## API Mapping Configuration

### Database Schema
The `api_mappings` table stores the mapping between SQL tables and API endpoints:

| Column | Type | Description |
|--------|------|-------------|
| table_name | VARCHAR(100) | SQL table name |
| api_url | VARCHAR(500) | Target API endpoint URL |
| method | VARCHAR(10) | HTTP method (GET, POST, etc.) |
| request_template | TEXT | JSON template for API requests |
| description | TEXT | Optional description |
| is_active | BOOLEAN | Whether mapping is active |

### Creating API Mappings

#### Example 1: Simple GET Request
```sql
INSERT INTO api_mappings (table_name, api_url, method, request_template, description, is_active)
VALUES (
  'users',
  'https://api.example.com/v1/users',
  'GET',
  '{"params": {"limit": 100}}',
  'User information API',
  true
);
```

#### Example 2: POST Request with Parameters
```sql
INSERT INTO api_mappings (table_name, api_url, method, request_template, description, is_active)
VALUES (
  'orders',
  'https://api.example.com/v1/orders/search',
  'POST',
  '{"user_id": "{{user_id}}", "status": "{{status}}"}',
  'Order search API',
  true
);
```

#### Example 3: Dynamic URL Parameters
```sql
INSERT INTO api_mappings (table_name, api_url, method, request_template, description, is_active)
VALUES (
  'user_details',
  'https://api.example.com/v1/users/{{id}}',
  'GET',
  '{}',
  'Individual user details',
  true
);
```

### Parameter Injection
The system automatically injects SQL WHERE conditions into API requests:

**SQL Query:**
```sql
SELECT * FROM users WHERE id = 123 AND status = 'active'
```

**Results in API call:**
```http
GET https://api.example.com/v1/users?id=123&status=active
```

## Caching System

### How Caching Works
- **Cache Key**: MD5 hash of the SQL query
- **Cache TTL**: Configurable (default 300 seconds)
- **Cache Storage**: Redis
- **Background Updates**: Enabled for frequently accessed data

### Cache Management

#### Check Cache Status
```bash
curl http://localhost:8000/cache/stats
```

#### Clear Cache for Specific Query
```bash
curl -X DELETE http://localhost:8000/cache \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

#### Clear All Cache
```bash
curl -X DELETE http://localhost:8000/cache/all
```

### Cache Performance Tips
- Use consistent SQL formatting for better cache hit rates
- Configure appropriate TTL based on data freshness requirements
- Monitor cache hit rates and adjust TTL accordingly
- Use `LIMIT` clauses to reduce cache storage requirements

## Error Handling

### Common Error Types

#### 1. SQL Syntax Errors
```json
{
  "status": 400,
  "message": "SQL parsing error: Invalid syntax near 'FROM'"
}
```

#### 2. API Mapping Not Found
```json
{
  "status": 404,
  "message": "No API mapping found for table 'unknown_table'"
}
```

#### 3. API Call Failures
```json
{
  "status": 503,
  "message": "API call failed: Connection timeout"
}
```

#### 4. Rate Limiting
```json
{
  "status": 429,
  "message": "Rate limit exceeded. Please try again later."
}
```

### Error Handling Best Practices
1. **Log Errors**: All errors are logged to `sql2api.log`
2. **Retry Logic**: Automatic retry for transient failures
3. **Circuit Breaker**: Prevents cascading failures
4. **Graceful Degradation**: Returns partial results when possible

## Deployment

### Docker Deployment

#### Development Environment
```bash
cd sql2api-agent
docker-compose up -d
```

#### Production Environment
```bash
# Build production image
docker build -t nl2api:latest .

# Run with production configuration
docker run -d \
  --name nl2api \
  -p 8000:8000 \
  -e DB_HOST=production-db \
  -e REDIS_HOST=production-redis \
  -v /path/to/config:/app/config \
  nl2api:latest
```

### Scaling Considerations

#### Horizontal Scaling
- Deploy multiple instances behind a load balancer
- Use shared Redis cluster for caching
- Configure database connection pooling

#### Performance Optimization
- Set `MAX_CONCURRENT_REQUESTS` based on API limits
- Configure appropriate cache TTL values
- Use connection pooling for database connections
- Monitor and adjust worker processes

### Monitoring

#### Health Checks
```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

#### Metrics
- Request count and latency
- Cache hit rates
- API call success/failure rates
- Database query performance

## Troubleshooting

### Common Issues

#### 1. Service Won't Start
**Check logs:**
```bash
docker logs sql2api-agent-app-1
```

**Common causes:**
- Database connection issues
- Missing environment variables
- Port conflicts

#### 2. SQL Queries Not Working
**Check API mappings:**
```sql
SELECT * FROM api_mappings WHERE table_name = 'your_table';
```

**Check SQL syntax:**
```bash
curl -X POST http://localhost:8000/parse \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

#### 3. API Calls Failing
**Check API endpoint:**
```bash
curl -I https://your-api-endpoint.com
```

**Check request template:**
```bash
curl http://localhost:8000/mappings/users
```

#### 4. Performance Issues
**Check cache performance:**
```bash
curl http://localhost:8000/cache/stats
```

**Check slow queries:**
```bash
tail -f sql2api-agent/logs/sql2api.log | grep "execution_time"
```

### Debug Mode
Enable debug mode for detailed logging:
```bash
export LOG_LEVEL=DEBUG
docker-compose restart
```

### Support
For additional help:
1. Check the logs in `sql2api-agent/logs/sql2api.log`
2. Review API mappings configuration
3. Verify network connectivity to target APIs
4. Check Docker container status
5. Review system resource usage

---

**Version**: 1.0.0
**Last Updated**: 2025-01-18
**Documentation**: Generated by Claude Code
