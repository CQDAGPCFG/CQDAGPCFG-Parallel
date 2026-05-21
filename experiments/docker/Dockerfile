FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace/CQDAGPCFG-Parallel/experiments/src:/workspace/CQDAGPCFG-Parallel/src:/workspace

WORKDIR /workspace

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir pyzmq pytest

WORKDIR /workspace/CQDAGPCFG-Parallel
