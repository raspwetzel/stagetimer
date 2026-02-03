"""
Datenbank-Management für StageTimer
SQLite-basierte Persistenz für Bands, Historie, Benutzer, Logos und Einstellungen
"""

import sqlite3
from datetime import datetime
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

DB_FILE = 'stagetimer.db'


@contextmanager
def get_db():
    """Context Manager für Datenbankverbindungen"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Ermöglicht dict-ähnlichen Zugriff
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()


def init_database():
    """Initialisiert die Datenbank mit allen Tabellen"""
    with get_db() as conn:
        cursor = conn.cursor()

        # Tabelle: bands (aktueller Schedule)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                band_name TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                duration INTEGER NOT NULL,
                end_date TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabelle: history (gespielte Bands)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                band_name TEXT NOT NULL,
                scheduled_date TEXT NOT NULL,
                scheduled_start TEXT NOT NULL,
                scheduled_end TEXT NOT NULL,
                actual_start DATETIME NOT NULL,
                actual_end DATETIME,
                duration INTEGER,
                hidden INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Index für schnellere Historie-Abfragen
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_history_hidden
            ON history(hidden, created_at DESC)
        ''')

        # Tabelle: users (Benutzer)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabelle: band_logos (Logo-Zuordnungen)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS band_logos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                band_name TEXT UNIQUE NOT NULL,
                logo_filename TEXT NOT NULL,
                uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabelle: settings (Key-Value Store)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        logger.info("Database initialized successfully")


# ==================== BANDS (Schedule) ====================

def get_all_bands():
    """Gibt alle Bands aus dem Schedule zurück, sortiert nach Datum und Zeit"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, date, band_name, start_time, end_time, duration, end_date
            FROM bands
            ORDER BY date, start_time
        ''')
        return [dict(row) for row in cursor.fetchall()]


def add_band(date, band_name, start_time, end_time, duration, end_date):
    """Fügt eine neue Band zum Schedule hinzu"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO bands (date, band_name, start_time, end_time, duration, end_date)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (date, band_name, start_time, end_time, duration, end_date))
        return cursor.lastrowid


def update_band(band_id, date, band_name, start_time, end_time, duration, end_date):
    """Aktualisiert eine bestehende Band"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE bands
            SET date = ?, band_name = ?, start_time = ?, end_time = ?, duration = ?, end_date = ?
            WHERE id = ?
        ''', (date, band_name, start_time, end_time, duration, end_date, band_id))


def delete_band(band_id):
    """Löscht eine Band aus dem Schedule"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM bands WHERE id = ?', (band_id,))


def delete_all_bands():
    """Löscht alle Bands aus dem Schedule"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM bands')


# ==================== HISTORY ====================

def add_to_history(band_name, scheduled_date, scheduled_start, scheduled_end,
                   actual_start, actual_end=None, duration=None):
    """Fügt einen Band-Auftritt zur Historie hinzu"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO history
            (band_name, scheduled_date, scheduled_start, scheduled_end,
             actual_start, actual_end, duration, hidden)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0)
        ''', (band_name, scheduled_date, scheduled_start, scheduled_end,
              actual_start, actual_end, duration))
        return cursor.lastrowid


def get_visible_history(limit=50):
    """Gibt sichtbare Historie-Einträge zurück (hidden=0)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, band_name, scheduled_date, scheduled_start, scheduled_end,
                   actual_start, actual_end, duration, created_at
            FROM history
            WHERE hidden = 0
            ORDER BY created_at DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]


def get_all_history(limit=100):
    """Gibt alle Historie-Einträge zurück (auch versteckte)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, band_name, scheduled_date, scheduled_start, scheduled_end,
                   actual_start, actual_end, duration, hidden, created_at
            FROM history
            ORDER BY created_at DESC
            LIMIT ?
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]


def hide_history_entry(history_id):
    """Versteckt einen Historie-Eintrag (soft delete)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE history SET hidden = 1 WHERE id = ?', (history_id,))


def hide_all_history():
    """Versteckt alle Historie-Einträge (soft delete)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE history SET hidden = 1 WHERE hidden = 0')


def unhide_history_entry(history_id):
    """Macht einen Historie-Eintrag wieder sichtbar"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE history SET hidden = 0 WHERE id = ?', (history_id,))


def delete_history_entry_permanently(history_id):
    """Löscht einen Historie-Eintrag permanent (nur für Admin)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM history WHERE id = ?', (history_id,))


# ==================== USERS ====================

def get_all_users():
    """Gibt alle Benutzer zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, username FROM users ORDER BY username')
        return [dict(row) for row in cursor.fetchall()]


def get_user(username):
    """Gibt einen spezifischen Benutzer zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, username, password_hash FROM users WHERE username = ?',
                      (username,))
        row = cursor.fetchone()
        return dict(row) if row else None


def add_user(username, password_hash):
    """Fügt einen neuen Benutzer hinzu"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO users (username, password_hash) VALUES (?, ?)',
                      (username, password_hash))
        return cursor.lastrowid


def delete_user(username):
    """Löscht einen Benutzer"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM users WHERE username = ?', (username,))


# ==================== BAND LOGOS ====================

def get_all_band_logos():
    """Gibt alle Band-Logo-Zuordnungen zurück als Dictionary {band_name: filename}"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT band_name, logo_filename FROM band_logos')
        return {row['band_name']: row['logo_filename'] for row in cursor.fetchall()}


def get_band_logo(band_name):
    """Gibt das Logo für eine bestimmte Band zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT logo_filename FROM band_logos WHERE band_name = ?',
                      (band_name,))
        row = cursor.fetchone()
        return row['logo_filename'] if row else None


def set_band_logo(band_name, logo_filename):
    """Setzt oder aktualisiert das Logo für eine Band"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO band_logos (band_name, logo_filename, uploaded_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(band_name) DO UPDATE SET
                logo_filename = excluded.logo_filename,
                uploaded_at = CURRENT_TIMESTAMP
        ''', (band_name, logo_filename))


def delete_band_logo(band_name):
    """Löscht die Logo-Zuordnung für eine Band"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM band_logos WHERE band_name = ?', (band_name,))


def rename_band_in_logos(old_name, new_name):
    """Benennt eine Band in der Logo-Zuordnung um (für Smart Rename)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE band_logos SET band_name = ? WHERE band_name = ?
        ''', (new_name, old_name))


# ==================== SETTINGS ====================

def get_setting(key, default=None):
    """Gibt einen Einstellungs-Wert zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        return row['value'] if row else default


def set_setting(key, value):
    """Setzt oder aktualisiert eine Einstellung"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO settings (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = CURRENT_TIMESTAMP
        ''', (key, value))


def get_all_settings():
    """Gibt alle Einstellungen als Dictionary zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT key, value FROM settings')
        return {row['key']: row['value'] for row in cursor.fetchall()}


# ==================== MIGRATION HELPERS ====================

def import_bands_from_list(bands_list):
    """Importiert Bands aus einer Liste (für Migration von CSV)"""
    delete_all_bands()
    for band in bands_list:
        add_band(
            date=band['date'],
            band_name=band['band'],
            start_time=band['start'],
            end_time=band['end'],
            duration=band['duration'],
            end_date=band.get('end_date', band['date'])
        )
    logger.info(f"Imported {len(bands_list)} bands from CSV")


def import_users_from_dict(users_dict):
    """Importiert Benutzer aus einem Dictionary oder einer Liste (für Migration von JSON)"""
    with get_db() as conn:
        cursor = conn.cursor()

        # Unterstütze beide Formate: Liste oder Dict mit 'users' Key
        if isinstance(users_dict, list):
            users_list = users_dict
        else:
            users_list = users_dict.get('users', [])

        for user in users_list:
            # Unterstütze beide Feld-Namen: 'password' und 'password_hash'
            password_hash = user.get('password_hash') or user.get('password')
            cursor.execute('''
                INSERT OR IGNORE INTO users (username, password_hash)
                VALUES (?, ?)
            ''', (user['username'], password_hash))
    logger.info(f"Imported {len(users_list)} users from JSON")


def import_band_logos_from_dict(logos_dict):
    """Importiert Band-Logos aus einem Dictionary (für Migration von JSON)"""
    for band_name, logo_filename in logos_dict.items():
        set_band_logo(band_name, logo_filename)
    logger.info(f"Imported {len(logos_dict)} band logos from JSON")


def import_settings_from_dict(settings_dict):
    """Importiert Einstellungen aus einem Dictionary (für Migration von JSON)"""
    for key, value in settings_dict.items():
        # Konvertiere Werte zu Strings für Key-Value Store
        set_setting(key, str(value))
    logger.info(f"Imported {len(settings_dict)} settings")
