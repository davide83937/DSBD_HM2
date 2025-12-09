FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x entrypoint.sh

# Esponi porte (Flask e gRPC)
EXPOSE 5005 50052

CMD ["./entrypoint.sh"]