FROM python:3.9-slim

WORKDIR /workspace

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash vscode
USER vscode

COPY --chown=vscode:vscode requirements.txt .
RUN pip install --user -r requirements.txt
ENV PATH="/home/vscode/.local/bin:${PATH}"

COPY --chown=vscode:vscode . .