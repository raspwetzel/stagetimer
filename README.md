# StageTimer

Eine umfassende Webanwendung zur Verwaltung und Anzeige von Band-Zeitplänen für Live-Events wie Konzerte und Festivals. Mit SQLite-Datenbank, Band-Logo-Integration, automatischer Historie und CSV-Import/Export.

## Features

### Core-Features
- **Live-Timer-Display**: Öffentliche Anzeige mit Countdown und Fortschrittsbalken
- **Admin-Dashboard**: Verwaltung des Event-Zeitplans mit Echtzeit-Vorschau
- **Echtzeit-Synchronisation**: WebSocket-basierte Updates zwischen Admin und Display
- **SQLite-Datenbank**: Robuste Datenhaltung mit automatischer Backup-Funktion
- **Benutzer-Authentifizierung**: Multi-User-Zugriff mit Rollenverwaltung
- **Responsive Design**: Funktioniert auf Smartphones, Tablets und Desktop-Displays

### Neue Features (v2.0)
- **Band-Logos**: Upload und Anzeige von Band-Logos auf der Bühnen-Anzeige
  - Smart Rename: Automatische Übertragung beim Umbenennen von Bands
  - Unterstützte Formate: PNG, JPG, GIF, SVG, WEBP
- **Historie-System**: Automatische Aufzeichnung aller gespielten Sets
  - Geplante vs. tatsächliche Zeiten
  - Soft-Delete Funktion (Einträge bleiben in DB)
  - Export-fähig für Auswertungen
- **CSV-Verwaltung**: Import und Export des gesamten Zeitplans
  - Beispiel-CSV-Download (mit aktuellem Datum)
  - Konflikterkennung beim Upload
  - Bulk-Import für große Events
- **Anleitung/Hilfeseite**: Integrierte Dokumentation aller Features
- **Datenbank-Migration**: Automatisches Migrations-Script von CSV/JSON zu SQLite

### Weitere Features
- **Logo-Anpassung**: Upload und Positionierung von Custom-Branding
- **Warnsystem**: Farbcodierte Indikatoren (grün → orange → rot)
- **Wake-Lock**: Verhindert Screen-Sleep auf mobilen Geräten
- **Docker-Ready**: Einfaches Deployment mit Docker und Docker Compose
- **Zeit-Anpassung**: Live-Verlängerung/Verkürzung während des Events
- **Backstage-View**: Kompakte Ansicht für Backstage-Monitore
- **Timetable-View**: Öffentliche Zeitplan-Übersicht

## Technologie-Stack

- **Backend**: Flask 3.0.2, Flask-SocketIO 5.3.6
- **Database**: SQLite3 (mit automatischer Migration)
- **Authentication**: Flask-Login 0.6.3
- **Data Processing**: Pandas 2.2.1
- **Real-time**: Socket.IO (WebSockets)
- **Frontend**: HTML5, CSS3, Vanilla JavaScript
- **Containerization**: Docker, Docker Compose

## Installation

### Voraussetzungen

- Python 3.12+ oder Docker
- Git

### Option 1: Lokale Installation

1. **Repository klonen**
   ```bash
   git clone <repository-url>
   cd stagetimer
   ```

2. **Virtual Environment erstellen**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # oder
   venv\Scripts\activate     # Windows
   ```

3. **Dependencies installieren**
   ```bash
   pip install -r requirements.txt
   ```

4. **Umgebungsvariablen konfigurieren**
   ```bash
   cp .env.example .env
   # Bearbeite .env und setze SECRET_KEY
   ```

5. **Erste Migration (falls vorhanden)**
   ```bash
   # Falls CSV/JSON-Dateien existieren, führe Migration aus
   python migrate_to_db.py
   ```

6. **Anwendung starten**
   ```bash
   python app.py
   ```

7. **Browser öffnen**
   - Display: http://localhost:5000
   - Admin: http://localhost:5000/admin
   - Anleitung: http://localhost:5000/guide (nach Login)

### Option 2: Docker Installation

1. **Repository klonen**
   ```bash
   git clone <repository-url>
   cd stagetimer
   ```

2. **Umgebungsvariablen konfigurieren**
   ```bash
   cp .env.example .env
   # Bearbeite .env und setze SECRET_KEY
   ```

3. **Container starten**
   ```bash
   docker-compose up -d
   ```

4. **Logs anzeigen**
   ```bash
   docker-compose logs -f
   ```

5. **Container stoppen**
   ```bash
   docker-compose down
   ```

## Konfiguration

### Umgebungsvariablen

Erstelle eine `.env` Datei basierend auf `.env.example`:

```bash
# Flask Secret Key (WICHTIG: In Produktion ändern!)
SECRET_KEY=your_secret_key_here

# Flask Environment
FLASK_ENV=production

# Timezone
TZ=Europe/Berlin

# Log Level
LOG_LEVEL=INFO
```

**Secret Key generieren**:
```bash
python -c "import os; print(os.urandom(24).hex())"
```

### Benutzer verwalten

**Neuen Benutzer erstellen**:

Ab Version 2.0 werden Benutzer in der SQLite-Datenbank verwaltet. Neue Benutzer können direkt im Admin-Panel unter "Benutzerverwaltung" erstellt werden.

Alternativ via Python:
```python
from database import add_user
from werkzeug.security import generate_password_hash

add_user('username', generate_password_hash('password'))
```

**Standard-Benutzer** (WICHTIG: Passwörter ändern!):
- Username: `admin` / Password: (siehe users.json vor Migration)

### Zeitplan konfigurieren

Der Zeitplan wird in der SQLite-Datenbank (`stagetimer.db`) gespeichert.

**Option 1: Über Admin-Panel** (empfohlen)
- Einzelne Bands unter "Neuen Slot anlegen" hinzufügen
- Direkt in der Zeitplan-Tabelle bearbeiten

**Option 2: CSV-Import**
1. Beispiel-CSV herunterladen (Admin → CSV Verwaltung)
2. Anpassen und hochladen

CSV-Format:
```csv
date,band,start,end
2026-02-02,Band Name,19:00,20:00
2026-02-02,Next Band,20:30,22:00
```

- **date**: Datum im Format YYYY-MM-DD
- **band**: Name der Band
- **start**: Startzeit im Format HH:MM
- **end**: Endzeit im Format HH:MM
- **Hinweis**: Dauer und end_date werden automatisch berechnet

### Migration von alter Version

Falls du von einer älteren Version (mit CSV/JSON-Dateien) migrierst:

```bash
# Backup erstellen (automatisch)
python migrate_to_db.py

# Alte Dateien werden in 'backups/' verschoben
# Die neue Datenbank wird automatisch erstellt
```

## Verwendung

### Öffentliches Display

Öffne `http://localhost:5000` für die öffentliche Timer-Anzeige.

**Features**:
- Großer, lesbarer Timer
- Fortschrittsbalken mit Farbcodierung
- Automatische Anzeige der nächsten Band
- Fullscreen-Button
- Wake-Lock für mobile Geräte

### Admin-Panel

Öffne `http://localhost:5000/admin` für das Admin-Dashboard.

**Features**:
- **Live-Vorschau**: Echtzeit-Anzeige des aktuellen Timer-Status
- **Zeitplan-Verwaltung**: Bands hinzufügen, bearbeiten, löschen
- **Band-Logos**: Logo-Upload pro Band mit Smart Rename
- **CSV-Verwaltung**: Import/Export des gesamten Zeitplans
- **Historie**: Automatische Aufzeichnung aller gespielten Sets
- **Zeit-Anpassung**: +/- 1/5/10 Minuten während des Events
- **Nachrichten**: Kurze Warnungen an Band (10/15/30 Sek)
- **Logo-Einstellungen**: Stage-Logo hochladen und Größe anpassen
- **Warnzeiten**: Orange/Rot-Warnung konfigurieren
- **Benutzerverwaltung**: Neue Benutzer erstellen und löschen
- **Theme**: Hell/Dunkel-Modus umschalten

### Backstage-View

Öffne `http://localhost:5000/backstage` für die kompakte Backstage-Ansicht.

**Ideal für:**
- Backstage-Monitore
- Techniker-Displays
- Mobile Geräte der Crew

### Timetable-View

Öffne `http://localhost:5000/timetable` für die öffentliche Zeitplan-Übersicht.

**Features:**
- Übersicht aller Bands des Tages
- Sortiert nach Startzeit
- Responsive Design

### Anleitung

Öffne `http://localhost:5000/guide` (nach Login) für die vollständige Dokumentation.

**Beinhaltet:**
- Detaillierte Feature-Erklärungen
- Best Practices
- Tipps für Events
- Fehlersuche und Support

## Sicherheit

### Best Practices

1. **Secret Key**: Immer einen sicheren, zufälligen Secret Key in Produktion verwenden
2. **Passwörter**: Standard-Passwörter vor dem ersten Einsatz ändern
3. **HTTPS**: In Produktion immer HTTPS verwenden (z.B. mit nginx + Let's Encrypt)
4. **Firewall**: Port 5000 nur für autorisierte IPs freigeben
5. **Updates**: Regelmäßig Dependencies aktualisieren

### Passwort-Hashing

Die Anwendung verwendet Werkzeug's `generate_password_hash` mit folgenden Einstellungen:
- Methode: scrypt (sicher und modern)
- Automatische Salt-Generierung

## Produktions-Deployment

### Mit Docker

```bash
# .env konfigurieren
cp .env.example .env
nano .env  # SECRET_KEY setzen

# Container starten
docker-compose up -d

# Reverse Proxy (nginx) konfigurieren
# Beispiel nginx-Konfiguration:
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:5000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

### Mit Gunicorn (Production WSGI Server)

```bash
# Gunicorn installieren
pip install gunicorn eventlet

# Server starten
gunicorn --worker-class eventlet -w 1 --bind 0.0.0.0:5000 app:app
```

## Fehlersuche

### Logs anzeigen

**Docker**:
```bash
docker-compose logs -f stagetimer
```

**Lokal**:
Logs werden auf der Konsole ausgegeben.

### Häufige Probleme

**Problem**: WebSocket-Verbindung schlägt fehl
**Lösung**: Prüfe, ob Port 5000 erreichbar ist und ob ein Reverse Proxy WebSockets unterstützt

**Problem**: Timer startet nicht automatisch
**Lösung**: Prüfe das Datum in schedule.csv (muss YYYY-MM-DD sein)

**Problem**: Upload-Fehler
**Lösung**: Prüfe Berechtigungen für `static/uploads/` Verzeichnis

## Entwicklung

### Entwicklungsumgebung

```bash
# Virtual Environment aktivieren
source venv/bin/activate

# Development-Server mit Debug-Modus
export FLASK_ENV=development
python app.py
```

### Code-Struktur

```
stagetimer/
├── app.py                      # Hauptanwendung (Flask)
├── database.py                 # Datenbank-Management (SQLite)
├── migrate_to_db.py           # Migrations-Script (CSV/JSON → SQLite)
├── hash.py                     # Passwort-Hash-Generator
├── requirements.txt            # Python-Dependencies
├── Dockerfile                 # Docker-Image
├── docker-compose.yml         # Docker-Compose-Konfiguration
├── .env.example               # Umgebungsvariablen-Vorlage
├── .gitignore                # Git-Ignorierte Dateien
├── stagetimer.db             # SQLite-Datenbank (auto-erstellt)
├── static/                   # Statische Dateien
│   ├── style.css             # Globale Styles
│   ├── script.js             # Legacy JavaScript
│   ├── socket.io.js          # Socket.IO Client
│   └── uploads/              # Hochgeladene Dateien
│       └── band_logos/       # Band-Logos
├── templates/                # HTML-Templates
│   ├── index.html            # Stage View (Hauptanzeige)
│   ├── backstage.html        # Backstage View
│   ├── timetable.html        # Timetable View
│   ├── admin.html            # Admin-Dashboard
│   ├── login.html            # Login-Seite
│   └── guide.html            # Anleitung/Hilfeseite
└── backups/                  # Auto-Backups bei Migration
    ├── schedule.csv.TIMESTAMP.backup
    ├── users.json.TIMESTAMP.backup
    ├── band_logos.json.TIMESTAMP.backup
    └── logo.json.TIMESTAMP.backup
```

## API

### REST Endpoints

#### Öffentliche Routen
**GET /**
- Stage View (Hauptanzeige)

**GET /backstage**
- Backstage View (kompakte Ansicht)

**GET /timetable**
- Timetable View (Zeitplan-Übersicht)

**GET /status**
- JSON-Status der aktuellen Band
- Response: `{status: "playing|waiting|finished", ...}`

#### Admin-Routen (Login erforderlich)
**GET/POST /admin**
- Admin-Dashboard

**GET /guide**
- Anleitung/Hilfeseite

**GET /download_example_csv**
- Beispiel-CSV herunterladen (mit aktuellem Datum)

**POST /upload_csv**
- CSV-Datei hochladen und importieren

**POST /upload_band_logo**
- Band-Logo hochladen

**POST /delete_band_logo**
- Band-Logo löschen

**GET /api/history**
- Historie abrufen (JSON)

**POST /api/history/hide**
- Einzelnen Historie-Eintrag verstecken

**POST /api/history/hide_all**
- Gesamte Historie leeren

#### Auth-Routen
**GET/POST /login**
- Benutzer-Login

**GET /logout**
- Benutzer-Logout

### WebSocket Events

**Empfangen: `time_update`**
```javascript
{
  status: "playing",
  band: "Band Name",
  remaining: 2700,  // Sekunden
  total_duration: 3600,
  warn_orange: 5,
  warn_red: 1,
  band_logo: "filename.png",  // Optional
  next_band: {                // Optional
    band: "Next Band",
    start: "20:00",
    date: "2026-02-02",
    countdown: 3600
  }
}
```

## Datenbank-Schema

Die SQLite-Datenbank enthält folgende Tabellen:

### bands (Aktueller Zeitplan)
```sql
id INTEGER PRIMARY KEY
date TEXT
band_name TEXT
start_time TEXT
end_time TEXT
duration INTEGER
end_date TEXT
created_at DATETIME
```

### history (Gespielte Sets)
```sql
id INTEGER PRIMARY KEY
band_name TEXT
scheduled_date TEXT
scheduled_start TEXT
scheduled_end TEXT
actual_start DATETIME
actual_end DATETIME
duration INTEGER
hidden INTEGER (0=sichtbar, 1=versteckt)
created_at DATETIME
```

### users (Benutzer)
```sql
id INTEGER PRIMARY KEY
username TEXT UNIQUE
password_hash TEXT
created_at DATETIME
```

### band_logos (Logo-Zuordnungen)
```sql
id INTEGER PRIMARY KEY
band_name TEXT UNIQUE
logo_filename TEXT
uploaded_at DATETIME
```

### settings (Konfiguration)
```sql
key TEXT PRIMARY KEY
value TEXT
updated_at DATETIME
```

## Versionshistorie

Die vollständige Versionshistorie mit allen Änderungen findest du in der [CHANGELOG.md](CHANGELOG.md).

**Aktuelle Version:** 2.0.1

### Highlights v2.0.1 (2026-02-03)
- Favicon-Integration für alle Plattformen
- Dark Mode für Anleitung-Seite
- MM:SS Countdown-Format überall
- Orange Next-Band Anzeige
- Automatisches Entfernen aus Zeitplan
- Verbesserte Historie mit Original-Zeiten
- Design-Verbesserungen und Bug-Fixes

### Highlights v2.0.0 (2026-02-02)
- SQLite-Datenbank statt CSV/JSON
- Band-Logo-Integration
- Historie-System
- CSV-Verwaltung mit Import/Export
- Docker-Support
- Anleitung/Hilfeseite

## Lizenz

[Deine Lizenz hier einfügen]

## Support

Bei Fragen oder Problemen erstelle bitte ein Issue im GitHub-Repository.

## Credits

**Entwickler:** André Wetzel
**Projekt:** StageTimer - Webbasiertes Countdown-System für Live-Events
**Version:** 2.0.1
**Technologie:** Flask, Socket.IO, SQLite, Docker
