FROM python:3.9-slim

# Install PostgreSQL development libraries
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the service files to the working directory
COPY app/ /app/

# Run the service
CMD ["python", "main.py"]