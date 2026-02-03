FROM python:3.12-slim

WORKDIR /app

# Installiere System-Dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Kopiere requirements.txt zuerst für besseres Layer Caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Kopiere Anwendungscode
COPY app.py .
COPY database.py .
COPY migrate_to_db.py .
COPY hash.py .

# Kopiere statische Dateien und Templates
COPY static/ static/
COPY templates/ templates/

# Erstelle Upload- und Backup-Verzeichnisse und setze Berechtigungen
RUN mkdir -p static/uploads backups && \
    chmod 755 static/uploads backups

# Erstelle Benutzer für Anwendung (Security Best Practice)
RUN useradd -m -u 1000 stagetimer && \
    chown -R stagetimer:stagetimer /app

USER stagetimer

# Setze Umgebungsvariablen
ENV FLASK_APP=app.py
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Port freigeben
EXPOSE 5000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/status')"

# Starte die Anwendung
CMD ["python", "app.py"] 