"""
Migrations-Script für StageTimer
Migriert Daten von CSV/JSON zu SQLite
"""

import os
import json
import shutil
import pandas as pd
from datetime import datetime
import database as db
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_backup(filename):
    """Erstellt ein Backup einer Datei mit Zeitstempel"""
    if not os.path.exists(filename):
        logger.info(f"Datei {filename} existiert nicht - wird übersprungen")
        return False

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = 'backups'
    os.makedirs(backup_dir, exist_ok=True)

    backup_path = os.path.join(backup_dir, f"{filename}.{timestamp}.backup")
    shutil.copy2(filename, backup_path)
    logger.info(f"✓ Backup erstellt: {backup_path}")
    return True


def migrate_schedule():
    """Migriert schedule.csv zur Datenbank"""
    csv_file = 'schedule.csv'
    if not os.path.exists(csv_file):
        logger.warning(f"Keine {csv_file} gefunden - erstelle leere Tabelle")
        return

    try:
        df = pd.read_csv(csv_file)
        bands = []

        for _, row in df.iterrows():
            # Berechne end_date falls nicht vorhanden
            end_date = row.get('end_date', row['date'])

            band = {
                'date': row['date'],
                'band': row['band'],
                'start': row['start'],
                'end': row['end'],
                'duration': int(row.get('duration', 0)),  # Falls duration in CSV existiert
                'end_date': end_date
            }

            # Berechne duration falls nicht in CSV
            if band['duration'] == 0:
                try:
                    start_dt = datetime.strptime(f"{band['date']} {band['start']}", '%Y-%m-%d %H:%M')
                    end_dt = datetime.strptime(f"{end_date} {band['end']}", '%Y-%m-%d %H:%M')
                    band['duration'] = int((end_dt - start_dt).total_seconds() / 60)
                except:
                    band['duration'] = 60  # Fallback

            bands.append(band)

        db.import_bands_from_list(bands)
        logger.info(f"✓ {len(bands)} Bands aus CSV importiert")

    except Exception as e:
        logger.error(f"Fehler beim Importieren von schedule.csv: {e}")
        raise


def migrate_users():
    """Migriert users.json zur Datenbank"""
    json_file = 'users.json'
    if not os.path.exists(json_file):
        logger.warning(f"Keine {json_file} gefunden - erstelle leere Users-Tabelle")
        return

    try:
        with open(json_file, 'r') as f:
            users_data = json.load(f)

        db.import_users_from_dict(users_data)
        logger.info(f"✓ Benutzer aus {json_file} importiert")

    except Exception as e:
        logger.error(f"Fehler beim Importieren von {json_file}: {e}")
        raise


def migrate_band_logos():
    """Migriert band_logos.json zur Datenbank"""
    json_file = 'band_logos.json'
    if not os.path.exists(json_file):
        logger.warning(f"Keine {json_file} gefunden - erstelle leere Logo-Tabelle")
        return

    try:
        with open(json_file, 'r') as f:
            logos_data = json.load(f)

        db.import_band_logos_from_dict(logos_data)
        logger.info(f"✓ {len(logos_data)} Band-Logos aus {json_file} importiert")

    except Exception as e:
        logger.error(f"Fehler beim Importieren von {json_file}: {e}")
        raise


def migrate_settings():
    """Migriert Einstellungen aus logo.json und anderen Quellen"""
    settings = {}

    # Logo-Einstellungen
    logo_json = 'logo.json'
    if os.path.exists(logo_json):
        try:
            with open(logo_json, 'r') as f:
                logo_data = json.load(f)
            settings['logo_filename'] = logo_data.get('filename', '')
            settings['logo_size_percent'] = str(logo_data.get('size_percent', 10))
            logger.info(f"✓ Logo-Einstellungen aus {logo_json} gelesen")
        except Exception as e:
            logger.warning(f"Konnte {logo_json} nicht lesen: {e}")

    # Weitere Einstellungen könnten hier hinzugefügt werden
    # z.B. warn_orange, warn_red falls diese irgendwo gespeichert sind

    if settings:
        db.import_settings_from_dict(settings)
        logger.info(f"✓ {len(settings)} Einstellungen importiert")


def main():
    """Führt die komplette Migration durch"""
    print("=" * 60)
    print("StageTimer - Datenbank-Migration")
    print("=" * 60)
    print()

    # 1. Backups erstellen
    print("Schritt 1: Erstelle Backups...")
    files_to_backup = ['schedule.csv', 'users.json', 'band_logos.json', 'logo.json']
    backed_up = 0
    for file in files_to_backup:
        if create_backup(file):
            backed_up += 1
    print(f"✓ {backed_up} Dateien gesichert\n")

    # 2. Datenbank initialisieren
    print("Schritt 2: Initialisiere Datenbank...")
    db.init_database()
    print("✓ Datenbank-Schema erstellt\n")

    # 3. Daten migrieren
    print("Schritt 3: Migriere Daten...")
    migrate_schedule()
    migrate_users()
    migrate_band_logos()
    migrate_settings()
    print()

    # 4. Zusammenfassung
    print("=" * 60)
    print("Migration abgeschlossen!")
    print("=" * 60)
    print()
    print("Nächste Schritte:")
    print("1. Starte die Anwendung neu")
    print("2. Überprüfe ob alle Daten korrekt importiert wurden")
    print("3. Die alten Dateien befinden sich im 'backups' Ordner")
    print("4. Falls alles funktioniert, kannst du die alten Dateien löschen")
    print()
    print(f"Datenbank-Datei: {db.DB_FILE}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print()
        print("=" * 60)
        print("FEHLER bei der Migration!")
        print("=" * 60)
        print(f"Fehler: {e}")
        print()
        print("Die Backups befinden sich im 'backups' Ordner.")
        print("Bitte überprüfe die Fehlermeldung und versuche es erneut.")
        exit(1)
