FROM python:3.12

WORKDIR /app

# Installa dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia codice e script
COPY . .
RUN chmod +x entrypoint.sh

# Esponi porte (Flask e gRPC)
EXPOSE 5000 50051

# Avvia lo script ibrido
CMD ["./entrypoint.sh"]