FROM python:3.9-slim

RUN apt-get update && apt-get install -y git git-lfs

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt