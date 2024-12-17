#!/bin/bash

tables=("weather_raw" "dim_location" "dim_time" "weather_fact")

# Container and database details
CONTAINER_NAME="weather_postgres"    # Replace with your Docker container name
DB_NAME="weather_data"               # Replace with your database name
DB_USER="user"                       # Replace with your PostgreSQL username
EXPORT_DIR="/tmp"                    # Directory inside the container to save CSV files
LOCAL_DIR="$HOME/Downloads"          # Replace with your directory on your pc to copy files to

for table in "${tables[@]}"; do
    echo "Exporting table: $table"
    
    # Export table to a CSV file inside the container
    docker exec "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY $table TO '$EXPORT_DIR/$table.csv' DELIMITER ',' CSV HEADER;"
    
    # Copy the CSV file from the container to the local Downloads folder
    docker cp "$CONTAINER_NAME:$EXPORT_DIR/$table.csv" "$LOCAL_DIR/"
    
    echo "Table $table exported to $LOCAL_DIR/$table.csv"
done

echo "All tables exported successfully!"
