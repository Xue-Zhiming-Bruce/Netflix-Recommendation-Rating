FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including build tools
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    gcc \
    g++ \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements-mlops.txt .
RUN pip install --no-cache-dir -r requirements-mlops.txt

# Remove build dependencies to reduce image size
RUN apt-get update && apt-get remove -y \
    gcc \
    g++ \
    python3-dev \
    build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Install Kaggle CLI
RUN pip install kaggle

# Copy application code
COPY . .

# Create directories for data and Kaggle config
RUN mkdir -p /app/netflix-prize-data /app/datamart/bronze /root/.kaggle

# Set Kaggle config directory
ENV KAGGLE_CONFIG_DIR=/root/.kaggle

# Create entrypoint script
RUN echo '#!/bin/bash\n\
set -e\n\
# Download Netflix Prize data if not already present\n\
if [ ! -d "/app/netflix-prize-data" ] || [ -z "$(ls -A /app/netflix-prize-data)" ]; then\n\
    echo "Downloading Netflix Prize data..."\n\
    kaggle datasets download -d netflix-inc/netflix-prize-data -p netflix-prize-data --unzip\n\
    echo "Data downloaded successfully"\n\
else\n\
    echo "Netflix Prize data already exists"\n\
fi\n\
\n\
# Run the bronze processing script\n\
echo "Processing data..."\n\
python utils/bronze_processing.py\n\
\n\
# Keep container running\n\
echo "Data processing complete. Container ready."\n\
tail -f /dev/null' > /app/entrypoint.sh

# Make entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Expose port for potential web services
EXPOSE 8000

# Set entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]