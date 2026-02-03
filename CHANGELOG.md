# Changelog

Alle wichtigen Änderungen an diesem Projekt werden in dieser Datei dokumentiert.

## [2.1.1] - 2026-02-03

### Behoben
- **Kritischer Bug in start_timer()**: NameError behoben - `warn_orange` und `warn_red` wurden als nicht existierende Variablen verwendet statt der Funktionen `get_warn_orange()` und `get_warn_red()`
- Timer-Start funktioniert jetzt zuverlaessig ueber Admin-Panel
- Historie wird korrekt gefuellt nach Ablauf einer Band

## [2.1.0] - 2026-02-03

### Hinzugefuegt
- **Rollenbasiertes Berechtigungssystem** mit 5 Rollen:
  - `ViewerStage` - Nur Buhnenanzeige (index.html)
  - `ViewerBackstage` - Nur Backstage-Anzeige (backstage.html)
  - `ViewerTimetable` - Nur Zeitplan-Anzeige (timetable.html)
  - `Stagemanager` - Eingeschraenkter Admin-Zugriff mit allen Viewer-Rechten
  - `Admin` - Vollzugriff auf alle Funktionen
- **Veranstaltungspasswort** fuer anonymen Zugang zur Buhnenanzeige
  - Einstellbar im Admin-Panel
  - Separater Login-Tab auf der Login-Seite
  - Gibt nur ViewerStage-Berechtigung
- **Rollen-Verwaltung** im Admin-Panel
  - Klick auf Benutzername oeffnet Rollen-Modal
  - Checkboxen fuer Rollenzuweisung
  - Validierung: Viewer-Rollen kombinierbar, Stagemanager/Admin exklusiv
- **Geschuetzte Viewer-Seiten** - Alle Seiten erfordern jetzt Login
- **Logout-Button** in der Navigation aller Views
- Neue Datenbank-Tabellen: `roles`, `user_roles`
- Neue API-Endpoints:
  - `GET/POST /api/user/<username>/roles` - Rollenverwaltung
  - `GET/POST /api/settings/event-password` - Veranstaltungspasswort

### Geaendert
- **Login-Seite** mit Tabs fuer Benutzer-Login und Veranstaltungspasswort
- **Admin-Panel** zeigt Sektionen basierend auf Rolle:
  - Stagemanager sieht: Display-Vorschau, Band-Nachricht, Zeitplan, Historie, Anleitung
  - Admin sieht zusaetzlich: CSV-Verwaltung, Logo-Settings, Warnzeiten, Benutzerverwaltung
- Hardcodierte Admin-Checks (`admin`/`Andre`) durch Rollensystem ersetzt
- Navigation zeigt nur zugaengliche Seiten basierend auf Benutzerrolle
- Bestehende User `admin` und `Andre` werden automatisch zur Admin-Rolle migriert

### Behoben
- Benutzer koennen sich nicht mehr selbst loeschen
- Letzter Admin-Benutzer kann nicht degradiert werden
- Eigene Admin-Rolle kann nicht entfernt werden

## [2.0.1] - 2026-02-03

### Hinzugefügt
- Favicon-Set für alle Browser und Plattformen (PWA-ready)
- Dark Mode Support für Anleitung-Seite (synchronisiert mit Admin-Panel)
- Feature-Cards in Anleitung mit permanenten blauen Rahmen
- Verbesserte CSS-Variablen (`--card-bg`, `--text-muted`) für konsistentes Theming
- Copyright-Informationen mit Entwickler-Attribution (André Wetzel)

### Geändert
- MM:SS Countdown-Format überall (statt HH:MM:SS)
- Next-Band Anzeige jetzt orange (statt blau) mit verbessertem Logo-Handling
- Anleitung-Design überarbeitet und mit Admin-Panel-Theme vereinheitlicht
- Guide-Link im Admin-Panel ohne Icon (cleaner Look)
- Verbesserte Hover-Effekte für Feature-Cards mit Box-Shadow

### Behoben
- Countdown zeigte -106 Stunden statt korrekte Minuten
- Schedule-Updates werden jetzt live in allen Views reflektiert
- Bands werden automatisch aus Zeitplan entfernt nach Spielzeit
- Historie zeigt korrekt ursprünglich geplante vs. tatsächliche Zeiten
- HTTP 500 Fehler bei Zeitanpassung behoben
- WebSocket-Verbindung sendet jetzt `schedule_updated` Event

## [2.0.0] - 2026-02-02

### Hinzugefügt
- **SQLite-Datenbank** statt CSV/JSON-Dateien
  - Tabellen: bands, history, users, band_logos, settings
  - Automatische Migrations-Scripts (`migrate_to_db.py`)
- **Band-Logo-Integration**
  - Upload von Logos pro Band
  - Smart Rename: Automatische Übertragung bei Bandnamen-Änderung
  - Unterstützte Formate: PNG, JPG, GIF, SVG, WEBP
  - Anzeige auf Stage-Display
- **Historie-System**
  - Automatische Aufzeichnung aller gespielten Sets
  - Geplante vs. tatsächliche Zeiten
  - Soft-Delete Funktion (Einträge bleiben in DB)
  - Export-fähig für Auswertungen
- **CSV-Verwaltung**
  - Beispiel-CSV-Download mit aktuellem Datum
  - Konflikterkennung beim Upload
  - Bulk-Import für große Events
- **Anleitung/Hilfeseite** (`/guide`)
  - Vollständige Feature-Dokumentation
  - Best Practices für Events
  - Fehlersuche und Support-Informationen
- **Benutzerverwaltung** im Admin-Panel
  - Benutzer erstellen/löschen direkt in der UI
  - Passwort-Hashing mit Werkzeug
- **Docker-Support**
  - Dockerfile mit Multi-Stage-Build
  - docker-compose.yml für einfaches Deployment
  - Health-Check für Container-Monitoring
  - Persistent Volumes für DB und Uploads

### Geändert
- Komplett neue Datenbank-Architektur mit SQLite
- Verbesserte Admin-Panel-Oberfläche
- Optimierte Real-Time-Updates via WebSocket
- Bessere Fehlerbehandlung und Logging

### Behoben
- Performance-Probleme bei großen Zeitplänen
- Race Conditions bei gleichzeitigen Updates
- Memory Leaks bei langen Sessions

## [1.0.0] - 2024-02-13

### Hinzugefügt
- Grundlegende Timer-Funktionalität
- CSV-basierte Zeitplan-Verwaltung
- Admin-Panel für Zeitplan-Bearbeitung
- Stage-Display mit Live-Countdown
- Backstage-View für Crew
- Timetable-View für Publikum
- Benutzer-Authentifizierung (JSON-basiert)
- WebSocket-basierte Echtzeit-Updates
- Responsive Design für Mobile/Desktop
- Warnsystem (Orange/Rot-Anzeigen)
- Zeit-Anpassung während laufenden Events
- Nachrichten-System für Bands
- Logo-Upload für Stage-Branding
- Wake-Lock für Mobile-Displays
- Theme-Umschaltung (Hell/Dunkel)

---

## Entwickler-Informationen

**Autor:** André Wetzel
**Projekt:** StageTimer - Webbasiertes Countdown-System für Live-Events
**Technologie-Stack:** Flask, Socket.IO, SQLite, Docker

### Version-Schema
- **Major (X.0.0)**: Breaking Changes, neue Hauptfunktionen
- **Minor (0.X.0)**: Neue Features, keine Breaking Changes
- **Patch (0.0.X)**: Bug-Fixes, kleine Verbesserungen