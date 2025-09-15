FROM python:3.9-slim

WORKDIR /workspace

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set up a non-root user with a home directory
RUN useradd -m -s /bin/bash vscode
USER vscode

# Install Python packages as the non-root user
COPY --chown=vscode:vscode requirements.txt .
RUN pip install --user -r requirements.txt
ENV PATH="/home/vscode/.local/bin:${PATH}"

COPY --chown=vscode:vscode . .