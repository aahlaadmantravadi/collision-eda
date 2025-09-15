# Use the stable "Bullseye" version of the Python 3.9 dev container image
FROM mcr.microsoft.com/devcontainers/python:3.9-bullseye

# Set the working directory
WORKDIR /workspace

# Switch to a non-root user
USER vscode

# Copy requirements and install python packages
COPY --chown=vscode:vscode requirements.txt .
RUN pip install --user -r requirements.txt
ENV PATH="/home/vscode/.local/bin:${PATH}"

# Copy the rest of the project files
COPY --chown=vscode:vscode . .