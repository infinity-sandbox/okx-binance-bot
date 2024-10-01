#!/bin/bash

echo "Stopping backend container..."
docker-compose -f docker-compose.yml down --timeout 60 backend

echo "Building backend container..."
docker-compose -f docker-compose.yml build backend

echo "Installing backend container..."
docker-compose -f docker-compose.yml up --remove-orphans --force-recreate -d backend

echo "Backend container installed successfully!"
