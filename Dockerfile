# 1. Use an official Python runtime as a parent image
FROM python:3.12-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /app

# Install PDM
# hadolint ignore=DL3013
RUN pip install --no-cache-dir pdm

# Copy project definition and lock file
COPY pyproject.toml pdm.lock ./

# Install dependencies using PDM
# Using --prod to skip development dependencies
# Using --no-editable to install packages normally instead of editable links
RUN pdm install --prod --no-editable

# Copy the rest of the application code
COPY . .

# Expose port 8000 (standard for FastAPI apps, adjust if needed)
EXPOSE 8000

# Command to run the application using Uvicorn
# Adjust 'main:app' if your FastAPI app instance is defined elsewhere
# Use 0.0.0.0 to make it accessible from outside the container
CMD ["pdm", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"] 