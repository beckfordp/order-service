#!/bin/bash
set -e
echo "🚀 Starting service..."
if ! docker info > /dev/null 2>&1; then
    echo "⚠️  Docker not running."
    exit 1
fi
if [ -f "docker-compose.yml" ]; then
    docker-compose up -d
    echo "⏳ Waiting for Postgres..."
    until docker-compose exec -T db pg_isready -U user 2>/dev/null; do sleep 1; done
    echo "✅ Postgres ready"
fi
mvn clean package -DskipTests
mvn spring-boot:run -Dspring-boot.run.profiles=local
