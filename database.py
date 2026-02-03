"""
Datenbank-Management für StageTimer
SQLite-basierte Persistenz für Bands, Historie, Benutzer, Logos und Einstellungen
"""

import sqlite3
from datetime import datetime
from contextlib import contextmanager
import logging
from werkzeug.security import generate_password_hash, check_password_hash

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

        # Tabelle: roles (Rollen-Definition)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                description TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Tabelle: user_roles (Benutzer-Rollen-Verknüpfung)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                role_id INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
                UNIQUE(user_id, role_id)
            )
        ''')

        # Index für schnellere Rollen-Abfragen
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_user_roles_user_id
            ON user_roles(user_id)
        ''')

        logger.info("Database initialized successfully")

    # Initialisiere Standard-Rollen und migriere bestehende Admin-User
    init_roles()
    migrate_admin_users()


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
    """Löscht einen Benutzer (und seine Rollen durch CASCADE)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM users WHERE username = ?', (username,))


def update_user_password(username, new_password_hash):
    """Aktualisiert das Passwort eines Benutzers"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET password_hash = ? WHERE username = ?',
                      (new_password_hash, username))
        return cursor.rowcount > 0


def get_user_by_id(user_id):
    """Gibt einen Benutzer anhand der ID zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, username, password_hash FROM users WHERE id = ?',
                      (user_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


# ==================== ROLES ====================

def init_roles():
    """Initialisiert die Standard-Rollen (falls nicht vorhanden)"""
    default_roles = [
        ('ViewerStage', 'Zugriff nur auf Bühnenanzeige (index.html)'),
        ('ViewerBackstage', 'Zugriff nur auf Backstage-Anzeige (backstage.html)'),
        ('ViewerTimetable', 'Zugriff nur auf Zeitplan-Anzeige (timetable.html)'),
        ('Stagemanager', 'Eingeschränkter Admin-Zugriff mit allen Viewer-Rechten'),
        ('Admin', 'Vollzugriff auf alle Funktionen'),
    ]

    with get_db() as conn:
        cursor = conn.cursor()
        for name, description in default_roles:
            cursor.execute('''
                INSERT OR IGNORE INTO roles (name, description)
                VALUES (?, ?)
            ''', (name, description))
        logger.info("Default roles initialized")


def get_all_roles():
    """Gibt alle verfügbaren Rollen zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, name, description FROM roles ORDER BY id')
        return [dict(row) for row in cursor.fetchall()]


def get_role_by_name(name):
    """Gibt eine Rolle anhand des Namens zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, name, description FROM roles WHERE name = ?', (name,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_user_roles(username):
    """Gibt die Rollen eines Benutzers als Liste von Rollennamen zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT r.name
            FROM roles r
            JOIN user_roles ur ON r.id = ur.role_id
            JOIN users u ON u.id = ur.user_id
            WHERE u.username = ?
            ORDER BY r.id
        ''', (username,))
        return [row['name'] for row in cursor.fetchall()]


def get_user_roles_by_id(user_id):
    """Gibt die Rollen eines Benutzers anhand der User-ID zurück"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT r.name
            FROM roles r
            JOIN user_roles ur ON r.id = ur.role_id
            WHERE ur.user_id = ?
            ORDER BY r.id
        ''', (user_id,))
        return [row['name'] for row in cursor.fetchall()]


def set_user_roles(user_id, role_names):
    """Setzt die Rollen eines Benutzers (ersetzt alle bestehenden Rollen)"""
    with get_db() as conn:
        cursor = conn.cursor()

        # Lösche alle bestehenden Rollen des Users
        cursor.execute('DELETE FROM user_roles WHERE user_id = ?', (user_id,))

        # Füge neue Rollen hinzu
        for role_name in role_names:
            cursor.execute('''
                INSERT INTO user_roles (user_id, role_id)
                SELECT ?, id FROM roles WHERE name = ?
            ''', (user_id, role_name))

        logger.info(f"Updated roles for user {user_id}: {role_names}")


def add_role_to_user(user_id, role_name):
    """Fügt eine Rolle zu einem Benutzer hinzu"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO user_roles (user_id, role_id)
            SELECT ?, id FROM roles WHERE name = ?
        ''', (user_id, role_name))
        return cursor.rowcount > 0


def remove_role_from_user(user_id, role_name):
    """Entfernt eine Rolle von einem Benutzer"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM user_roles
            WHERE user_id = ? AND role_id = (SELECT id FROM roles WHERE name = ?)
        ''', (user_id, role_name))
        return cursor.rowcount > 0


def user_has_role(username, role_name):
    """Prüft ob ein Benutzer eine bestimmte Rolle hat"""
    roles = get_user_roles(username)
    return role_name in roles


def user_has_any_role(username, role_names):
    """Prüft ob ein Benutzer mindestens eine der angegebenen Rollen hat"""
    user_roles = get_user_roles(username)
    return any(role in user_roles for role in role_names)


def get_users_with_roles():
    """Gibt alle Benutzer mit ihren Rollen zurück"""
    users = get_all_users()
    for user in users:
        user['roles'] = get_user_roles(user['username'])
    return users


def count_admins():
    """Zählt die Anzahl der Benutzer mit Admin-Rolle"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(DISTINCT ur.user_id) as count
            FROM user_roles ur
            JOIN roles r ON r.id = ur.role_id
            WHERE r.name = 'Admin'
        ''')
        return cursor.fetchone()['count']


def validate_role_combination(role_names):
    """
    Validiert ob eine Rollen-Kombination gültig ist.
    Regeln:
    - Viewer-Rollen (ViewerStage, ViewerBackstage, ViewerTimetable) können kombiniert werden
    - Stagemanager und Admin sind exklusiv (keine anderen Rollen erlaubt)
    """
    viewer_roles = {'ViewerStage', 'ViewerBackstage', 'ViewerTimetable'}
    exclusive_roles = {'Stagemanager', 'Admin'}

    user_viewer_roles = set(role_names) & viewer_roles
    user_exclusive_roles = set(role_names) & exclusive_roles

    # Keine exklusiven Rollen -> nur Viewer-Rollen erlaubt
    if not user_exclusive_roles:
        return len(set(role_names) - viewer_roles) == 0

    # Genau eine exklusive Rolle und keine anderen
    if len(user_exclusive_roles) == 1 and len(role_names) == 1:
        return True

    # Mehr als eine exklusive Rolle oder Kombination mit anderen
    return False


# ==================== EVENT PASSWORD ====================

def get_event_password_hash():
    """Gibt den Hash des Veranstaltungspassworts zurück (oder None wenn nicht gesetzt)"""
    value = get_setting('event_password_hash')
    return value if value else None


def set_event_password(password):
    """Setzt das Veranstaltungspasswort (wird gehasht gespeichert)"""
    if password:
        password_hash = generate_password_hash(password)
        set_setting('event_password_hash', password_hash)
        logger.info("Event password has been set")
    else:
        clear_event_password()


def clear_event_password():
    """Löscht das Veranstaltungspasswort"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM settings WHERE key = 'event_password_hash'")
        logger.info("Event password has been cleared")


def verify_event_password(password):
    """Prüft ob das angegebene Passwort mit dem Veranstaltungspasswort übereinstimmt"""
    stored_hash = get_event_password_hash()
    if not stored_hash:
        return False
    return check_password_hash(stored_hash, password)


def is_event_password_enabled():
    """Prüft ob ein Veranstaltungspasswort gesetzt ist"""
    return get_event_password_hash() is not None


# ==================== MIGRATION HELPERS (ROLES) ====================

def migrate_admin_users():
    """
    Migriert bestehende 'admin' und 'Andre' Benutzer zur Admin-Rolle.
    Wird bei der Initialisierung aufgerufen.
    """
    admin_usernames = ['admin', 'Andre']

    with get_db() as conn:
        cursor = conn.cursor()

        for username in admin_usernames:
            # Prüfe ob User existiert
            cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
            user = cursor.fetchone()

            if user:
                user_id = user['id']
                # Prüfe ob bereits Rollen zugewiesen sind
                cursor.execute('SELECT COUNT(*) as count FROM user_roles WHERE user_id = ?', (user_id,))
                has_roles = cursor.fetchone()['count'] > 0

                # Nur wenn noch keine Rollen zugewiesen sind
                if not has_roles:
                    cursor.execute('''
                        INSERT OR IGNORE INTO user_roles (user_id, role_id)
                        SELECT ?, id FROM roles WHERE name = 'Admin'
                    ''', (user_id,))
                    logger.info(f"Migrated user '{username}' to Admin role")


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
