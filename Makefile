.PHONY: up down build run test

up:
	docker-compose up -d

down:
	docker-compose down

build:
	./gradlew build -x test

test:
	./gradlew test

run:
	./gradlew bootRun

test:
	@echo "🧪 Running unit tests..."
	./gradlew test
