# Use Python 3.10 base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies for GEE
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY . .

# Expose port (not strictly needed for batch execution, but good practice)
EXPOSE 8000

# Default command: run single algorithm
# This will be overridden by Airflow DockerOperator
CMD ["python", "-c", "print('STACD Backend Container Ready')"]
