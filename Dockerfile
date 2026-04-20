# Dockerfile
FROM continuumio/miniconda3:latest

WORKDIR /app

# Install system dependencies needed for GDAL and PostGIS client
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy and create conda environment
COPY environment.yaml .
RUN conda env create -f environment.yaml && conda clean -afy

# Make conda env the default
SHELL ["conda", "run", "-n", "geo-pipeline", "/bin/bash", "-c"]

# Copy project files
COPY . .

# Default entrypoint uses the conda environment
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "geo-pipeline"]