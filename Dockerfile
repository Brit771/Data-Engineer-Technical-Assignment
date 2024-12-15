# Use the official Python slim image for efficiency
FROM python:3.9-slim

# Install PostgreSQL development libraries
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script to the working directory
COPY config/ ./config/
COPY db/ ./db/
COPY extract/ ./extract/
COPY transform/ ./transform/
COPY load/ ./load/
COPY workflows/ ./workflows/
COPY main.py .

# Run the service
CMD ["python", "main.py"]