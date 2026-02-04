# StageTimer

Eine umfassende Webanwendung zur Verwaltung und Anzeige von Band-Zeitplänen für Live-Events wie Konzerte und Festivals. Mit SQLite-Datenbank, Band-Logo-Integration, automatischer Historie und CSV-Import/Export.

## Features

### Core-Features
- **Live-Timer-Display**: Oeffentliche Anzeige mit Countdown und Fortschrittsbalken
- **Admin-Dashboard**: Verwaltung des Event-Zeitplans mit Echtzeit-Vorschau
- **Echtzeit-Synchronisation**: WebSocket-basierte Updates zwischen Admin und Display
- **SQLite-Datenbank**: Robuste Datenhaltung mit automatischer Backup-Funktion
- **Rollenbasierte Zugriffskontrolle**: 5 Rollen mit feingranularen Berechtigungen
- **Veranstaltungspasswort**: Anonymer Zugang zur Buhnenanzeige fuer Gaeste
- **Responsive Design**: Funktioniert auf Smartphones, Tablets und Desktop-Displays

### Neue Features (v2.1)
- **Rollenbasiertes Berechtigungssystem** mit 5 Rollen:
  - `ViewerStage` - Nur Buhnenanzeige
  - `ViewerBackstage` - Nur Backstage-Anzeige
  - `ViewerTimetable` - Nur Zeitplan-Anzeige
  - `Stagemanager` - Eingeschraenkter Admin + alle Viewer-Rechte
  - `Admin` - Vollzugriff auf alle Funktionen
- **Veranstaltungspasswort**: Anonymer Zugang zur Buhnenanzeige
  - Konfigurierbar im Admin-Panel
  - Separater Login-Tab
- **Rollen-Verwaltung**: Klick auf Benutzername im Admin-Panel
- **Geschuetzte Views**: Alle Seiten erfordern Login

### Features (v2.0)
- **Band-Logos**: Upload und Anzeige von Band-Logos auf der Buhnen-Anzeige
  - Smart Rename: Automatische Uebertragung beim Umbenennen von Bands
  - Unterstuetzte Formate: PNG, JPG, GIF, SVG, WEBP
- **Historie-System**: Automatische Aufzeichnung aller gespielten Sets
  - Geplante vs. tatsaechliche Zeiten
  - Soft-Delete Funktion (Eintraege bleiben in DB)
  - Export-faehig fuer Auswertungen
- **CSV-Verwaltung**: Import und Export des gesamten Zeitplans
  - Beispiel-CSV-Download (mit aktuellem Datum)
  - Konflikterkennung beim Upload
  - Bulk-Import fuer grosse Events
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

4. **Anwendung starten**
   ```bash
   python app.py
   ```

5. **Browser oeffnen und Admin-Account erstellen**
   - Oeffne http://localhost:5000
   - Bei der ersten Installation erscheint automatisch die Setup-Seite
   - Erstelle deinen Admin-Account (Benutzername + Passwort)
   - Du wirst automatisch eingeloggt

6. **Fertig!**
   - Admin-Panel: http://localhost:5000/admin
   - Buhnenanzeige: http://localhost:5000
   - Anleitung: http://localhost:5000/guide

### Option 2: Docker Installation

1. **Repository klonen**
   ```bash
   git clone <repository-url>
   cd stagetimer
   ```

2. **Container starten**
   ```bash
   docker-compose up -d
   ```

3. **Browser oeffnen und Admin-Account erstellen**
   - Oeffne http://localhost:5000
   - Bei der ersten Installation erscheint automatisch die Setup-Seite
   - Erstelle deinen Admin-Account (Benutzername + Passwort)

4. **Nuetzliche Docker-Befehle**
   ```bash
   docker-compose logs -f      # Logs anzeigen
   docker-compose down         # Container stoppen
   docker-compose restart      # Container neustarten
   ```

## Konfiguration

### Umgebungsvariablen (optional)

**Die Anwendung funktioniert komplett ohne Konfiguration!** Alle Einstellungen haben sinnvolle Standardwerte.

Nur wenn du etwas anpassen moechtest:
```bash
cp .env.example .env
# Bearbeite .env nach Bedarf
```

**Verfuegbare Optionen:**

| Variable | Standard | Beschreibung |
|----------|----------|--------------|
| `SECRET_KEY` | Auto-generiert | Session-Key (wird automatisch in `.secret_key` gespeichert) |
| `FLASK_ENV` | `production` | `production` = optimiert, `development` = Debug + Auto-Reload |
| `LOG_LEVEL` | `ERROR` | `ERROR`, `WARNING`, `INFO` oder `DEBUG` |
| `TZ` | `Europe/Berlin` | Zeitzone fuer korrekte Zeitanzeige |
| `PORT` | `5000` | Server-Port |

**Empfehlungen:**
- **Produktion**: Standardwerte belassen (`FLASK_ENV=production`, `LOG_LEVEL=ERROR`)
- **Entwicklung**: `FLASK_ENV=development` und `LOG_LEVEL=DEBUG` fuer mehr Details

### Benutzer und Rollen verwalten

**Rollen-Uebersicht**:

| Rolle | Stage | Backstage | Timetable | Admin (eingeschraenkt) | Admin (voll) |
|-------|:-----:|:---------:|:---------:|:---------------------:|:------------:|
| ViewerStage | X | - | - | - | - |
| ViewerBackstage | - | X | - | - | - |
| ViewerTimetable | - | - | X | - | - |
| Stagemanager | X | X | X | X | - |
| Admin | X | X | X | X | X |

- **Viewer-Rollen** koennen kombiniert werden (z.B. ViewerStage + ViewerTimetable)
- **Stagemanager** und **Admin** sind exklusiv (keine anderen Rollen moeglich)

**Stagemanager sieht im Admin-Panel**:
- Display-Vorschau, Band-Nachricht, Zeitplan (ohne CSV-Reload), Historie, Anleitung

**Admin sieht zusaetzlich**:
- CSV-Verwaltung, Logo-Settings, Warnzeiten, Benutzerverwaltung, Veranstaltungspasswort

**Neuen Benutzer erstellen**:

1. Im Admin-Panel unter "Benutzerverwaltung"
2. Benutzername und Passwort eingeben
3. Auf Benutzername klicken um Rollen zuzuweisen

**Veranstaltungspasswort**:

Ermoeglicht Gaesten Zugang zur Buhnenanzeige ohne eigenen Account:
1. Im Admin-Panel unter "Veranstaltungspasswort"
2. Passwort setzen (min. 4 Zeichen)
3. Gaeste waehlen auf der Login-Seite den Tab "Veranstaltung"

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
**Lösung**: Prüfe Berechtigungen für `data/uploads/` Verzeichnis (Docker: Volume `stagetimer_data`)

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

#### Viewer-Routen (Login erforderlich)
**GET /** oder **GET /stage**
- Stage View (Hauptanzeige)
- Erfordert: ViewerStage, Stagemanager oder Admin

**GET /backstage**
- Backstage View (kompakte Ansicht)
- Erfordert: ViewerBackstage, Stagemanager oder Admin

**GET /timetable**
- Timetable View (Zeitplan-Uebersicht)
- Erfordert: ViewerTimetable, Stagemanager oder Admin

#### Oeffentliche Routen
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

**GET/POST /api/user/<username>/roles** (Admin)
- Benutzer-Rollen abrufen/setzen

**GET/POST /api/settings/event-password** (Admin)
- Veranstaltungspasswort verwalten

#### Auth-Routen
**GET/POST /login**
- Benutzer-Login (mit Tabs fuer User und Veranstaltungspasswort)

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

### roles (Rollen-Definition)
```sql
id INTEGER PRIMARY KEY
name TEXT UNIQUE
description TEXT
created_at DATETIME
```

### user_roles (Benutzer-Rollen-Verknuepfung)
```sql
id INTEGER PRIMARY KEY
user_id INTEGER (FK -> users)
role_id INTEGER (FK -> roles)
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

Die vollstaendige Versionshistorie findest du in der [CHANGELOG.md](CHANGELOG.md).

**Aktuelle Version:** 2.3.0

## Lizenz

Keine Lizenz, nur Privat nutzbar

## Support

Bei Fragen oder Problemen erstelle bitte ein Issue im GitHub-Repository.

## Credits

**Entwickler:** Andre Wetzel
**Projekt:** StageTimer - Webbasiertes Countdown-System fuer Live-Events
**Version:** 2.3.0
**Technologie:** Flask, Socket.IO, SQLite, Docker
