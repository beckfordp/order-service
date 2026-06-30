# Order Service

## Prerequisites

- Java 17+
- Apache Maven 3.8+
- Docker

## Quick Start

Build and run the full stack (application + database) using the quickstart script:

```bash
bash quickstart.sh
```

This builds the Docker image and starts both the application and PostgreSQL on port 8082.

## Development Mode

Run just the database in Docker and start the application natively for fast iteration without rebuilding Docker images.

**Step 1 — Start the database:**

```bash
docker-compose up -d
```

**Step 2 — Run the application:**

```bash
mvn spring-boot:run
```

The application starts on port 8082. Restart `mvn spring-boot:run` to pick up source changes.

**Stop the database when done:**

```bash
docker-compose down
```

## Database

Flyway runs database migrations automatically on startup. The initial schema (`V1__init.sql`) is applied on first run.

**Reset the database** (drops all data and re-runs migrations on next startup):

```bash
docker-compose down -v
docker-compose up -d
```

## API Documentation

The Order Service uses SpringDoc OpenAPI. Once the application is running, open Swagger UI in your browser:

```
http://localhost:8082/api/swagger-ui.html
```

Swagger UI documents all endpoints with request/response schemas and allows you to execute requests directly from the browser.

The raw OpenAPI JSON spec is available at:

```
http://localhost:8082/api/v3/api-docs
```
