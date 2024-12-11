# Use the official Python slim image for efficiency
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script and the .env file
COPY weather_service.py .
COPY .env .

# Run the service
CMD ["python", "weather_service.py"]
