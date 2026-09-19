FROM python:3.12-slim

WORKDIR /app
COPY exporter.py /app/exporter.py
USER 65532:65532

EXPOSE 9110
CMD ["python", "-u", "/app/exporter.py"]
