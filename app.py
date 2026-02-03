import os
import threading
import time
import json
import logging
from functools import wraps
import pandas as pd
import io
from datetime import datetime, timedelta, date
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, jsonify, send_file, Response, abort
from flask_socketio import SocketIO, emit
from flask_login import LoginManager, login_user, login_required, logout_user, UserMixin, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
import database as db

# Lade Umgebungsvariablen aus .env Datei
load_dotenv()

# Logging konfigurieren - Nur Errors anzeigen
log_level = os.environ.get('LOG_LEVEL', 'ERROR')
logging.basicConfig(
    level=getattr(logging, log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

# Werkzeug (Flask) Logger auch auf ERROR setzen
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)
# Secret Key aus Umgebungsvariable oder generiere einen sicheren Standard
app.secret_key = os.environ.get('SECRET_KEY', os.urandom(24).hex())
app.config['UPLOAD_FOLDER'] = 'static/uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
socketio = SocketIO(app)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

class User(UserMixin):
    """Benutzer-Klasse mit Rollen-Unterstützung"""

    def __init__(self, username, roles=None):
        self.id = username
        self._roles = roles or []
        self.is_event_user = False

    @property
    def roles(self):
        """Gibt die Rollen des Benutzers zurück (lädt bei Bedarf aus DB)"""
        if not self._roles:
            self._roles = db.get_user_roles(self.id)
        return self._roles

    def has_role(self, role_name):
        """Prüft ob der Benutzer eine bestimmte Rolle hat"""
        return role_name in self.roles

    def has_any_role(self, role_names):
        """Prüft ob der Benutzer mindestens eine der angegebenen Rollen hat"""
        return any(role in self.roles for role in role_names)

    def is_admin(self):
        """Prüft ob der Benutzer Admin ist"""
        return 'Admin' in self.roles

    def is_stagemanager(self):
        """Prüft ob der Benutzer Stagemanager ist"""
        return 'Stagemanager' in self.roles

    def can_access_stage(self):
        """Prüft ob der Benutzer Zugriff auf die Bühnenanzeige hat"""
        return self.has_any_role(['ViewerStage', 'Stagemanager', 'Admin'])

    def can_access_backstage(self):
        """Prüft ob der Benutzer Zugriff auf die Backstage-Anzeige hat"""
        return self.has_any_role(['ViewerBackstage', 'Stagemanager', 'Admin'])

    def can_access_timetable(self):
        """Prüft ob der Benutzer Zugriff auf die Zeitplan-Anzeige hat"""
        return self.has_any_role(['ViewerTimetable', 'Stagemanager', 'Admin'])

    def can_access_admin(self):
        """Prüft ob der Benutzer Zugriff auf das Admin-Panel hat"""
        return self.has_any_role(['Stagemanager', 'Admin'])


class EventUser(UserMixin):
    """Anonymer Benutzer via Veranstaltungspasswort - nur ViewerStage-Zugriff"""

    def __init__(self):
        self.id = '__event_user__'
        self._roles = ['ViewerStage']
        self.is_event_user = True

    @property
    def roles(self):
        return self._roles

    def has_role(self, role_name):
        return role_name in self._roles

    def has_any_role(self, role_names):
        return any(role in self._roles for role in role_names)

    def is_admin(self):
        return False

    def is_stagemanager(self):
        return False

    def can_access_stage(self):
        return True

    def can_access_backstage(self):
        return False

    def can_access_timetable(self):
        return False

    def can_access_admin(self):
        return False


@login_manager.user_loader
def load_user(user_id):
    """Lädt einen Benutzer für Flask-Login"""
    # Event-User (Veranstaltungspasswort)
    if user_id == '__event_user__':
        return EventUser()

    # Regulärer Benutzer
    user = db.get_user(user_id)
    if user:
        roles = db.get_user_roles(user_id)
        return User(user_id, roles=roles)
    return None


# ==================== ROLLEN-DECORATORS ====================

def role_required(*required_roles):
    """
    Decorator der mindestens eine der angegebenen Rollen erfordert.
    Verwendung: @role_required('Admin', 'Stagemanager')
    """
    def decorator(f):
        @wraps(f)
        @login_required
        def decorated_function(*args, **kwargs):
            if not current_user.has_any_role(required_roles):
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def admin_required(f):
    """Decorator für Admin-only Routes"""
    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.is_admin():
            abort(403)
        return f(*args, **kwargs)
    return decorated_function


def stagemanager_or_admin_required(f):
    """Decorator für Stagemanager oder Admin Routes"""
    @wraps(f)
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.can_access_admin():
            abort(403)
        return f(*args, **kwargs)
    return decorated_function


schedule = []
current_band_index = -1
timer_running = False
end_time = None

# Ursprünglich geplante Zeiten für Historie
original_scheduled_start = None
original_scheduled_end = None
original_scheduled_duration = None
schedule_conflicts = []  # Liste der Konflikte in der CSV

# Helper-Funktionen für Settings (werden aus DB geladen)
def get_logo_filename():
    return db.get_setting('logo_filename', None)

def get_logo_size_percent():
    return int(db.get_setting('logo_size_percent', '10'))

def get_warn_orange():
    return int(db.get_setting('warn_orange', '5'))

def get_warn_red():
    return int(db.get_setting('warn_red', '1'))

def get_band_logos():
    """Gibt alle Band-Logo-Zuordnungen zurück"""
    return db.get_all_band_logos()

# Config-Funktionen sind nun in get_* Helper-Funktionen integriert
# Settings werden direkt aus der DB gelesen/geschrieben

def calculate_duration_and_end_date(start_date, start_time, end_time):
    """
    Berechnet Dauer und End-Datum aus Start-Datum, Start-Zeit und End-Zeit.
    Erkennt automatisch, wenn die Band über Mitternacht spielt.

    Args:
        start_date: Start-Datum als String (YYYY-MM-DD)
        start_time: Start-Zeit als String (HH:MM)
        end_time: End-Zeit als String (HH:MM)

    Returns:
        tuple: (duration_minutes, end_date_string)
    """
    # Parse Start-Datum und -Zeit
    start_datetime = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")

    # Parse End-Zeit (zunächst am gleichen Tag)
    end_datetime = datetime.strptime(f"{start_date} {end_time}", "%Y-%m-%d %H:%M")

    # Wenn Endzeit <= Startzeit, dann ist es am nächsten Tag
    if end_datetime <= start_datetime:
        end_datetime += timedelta(days=1)

    # Berechne Dauer in Minuten
    duration_minutes = int((end_datetime - start_datetime).total_seconds() / 60)

    # End-Datum als String
    end_date = end_datetime.date().isoformat()

    return duration_minutes, end_date

def sort_schedule():
    """Sortiert den schedule nach Datum und Startzeit"""
    global schedule
    schedule.sort(key=lambda x: (x['date'], x['start']))

def load_schedule():
    """Lädt den Schedule aus der Datenbank"""
    global schedule, schedule_conflicts
    schedule_conflicts = []  # Zurücksetzen

    try:
        logger.info("=== Loading Schedule from Database ===")
        logger.info(f"Current date: {datetime.now()}")

        # Lade Bands aus der DB
        bands_from_db = db.get_all_bands()

        # Temporärer Schedule zum Prüfen
        temp_schedule = []
        conflicts_found = []

        for idx, band_dict in enumerate(bands_from_db):
            # Erstelle Schedule-Eintrag
            temp_entry = {
                "date": band_dict["date"],
                "band": band_dict["band_name"].strip(),
                "start": band_dict["start_time"],
                "end": band_dict["end_time"],
                "end_date": band_dict["end_date"],
                "duration": band_dict["duration"]
            }

            # Prüfe auf Zeitkonflikte mit bereits verarbeiteten Bands
            new_start = datetime.strptime(f"{band_dict['date']} {band_dict['start_time']}", "%Y-%m-%d %H:%M")
            new_end = datetime.strptime(f"{band_dict['end_date']} {band_dict['end_time']}", "%Y-%m-%d %H:%M")

            for other_band in temp_schedule:
                other_start = datetime.strptime(f"{other_band['date']} {other_band['start']}", "%Y-%m-%d %H:%M")
                other_end = datetime.strptime(f"{other_band['end_date']} {other_band['end']}", "%Y-%m-%d %H:%M")

                # Prüfe auf Überschneidung
                if (new_start < other_end and new_end > other_start):
                    conflict_msg = {
                        "row": idx + 2,  # Äquivalent zur CSV-Zeile
                        "band1": band_dict['band_name'],
                        "time1": f"{band_dict['start_time']}-{band_dict['end_time']}",
                        "band2": other_band['band'],
                        "time2": f"{other_band['start']}-{other_band['end']}"
                    }
                    conflicts_found.append(conflict_msg)
                    logger.error(f"KONFLIKT: '{band_dict['band_name']}' ({band_dict['start_time']}-{band_dict['end_time']}) überschneidet sich mit '{other_band['band']}' ({other_band['start']}-{other_band['end']})")

            temp_schedule.append(temp_entry)

        # Wenn Konflikte gefunden wurden, NICHT laden
        if conflicts_found:
            schedule_conflicts = conflicts_found
            logger.error(f"=== FEHLER: {len(conflicts_found)} Zeitkonflikt(e) gefunden! Schedule wurde NICHT geladen. ===")
            # Schedule bleibt beim alten Stand
        else:
            # Kein Konflikt - Schedule übernehmen
            schedule.clear()
            schedule.extend(temp_schedule)
            logger.info(f"Schedule loaded: {len(schedule)} bands from database")

    except Exception as e:
        logger.error(f"Fehler beim Laden des Schedules aus DB: {e}")
        schedule_conflicts = [{"error": str(e)}]

def save_schedule_to_db():
    """Speichert den aktuellen Schedule in die Datenbank"""
    try:
        # Lösche alle Bands aus der DB
        db.delete_all_bands()

        # Füge alle Bands wieder hinzu
        for band in schedule:
            db.add_band(
                date=band['date'],
                band_name=band['band'],
                start_time=band['start'],
                end_time=band['end'],
                duration=band['duration'],
                end_date=band['end_date']
            )

        logger.info(f"Schedule erfolgreich in DB gespeichert: {len(schedule)} bands")
    except Exception as e:
        logger.error(f"Fehler beim Speichern des Schedules in DB: {e}")
        raise

def find_next_band():
    now = datetime.now()
    current_date = now.date().isoformat()

    logger.debug("=== Finding Next Band ===")
    logger.debug(f"Current time: {now}")
    logger.debug(f"Current date: {current_date}")

    # Count bands for today
    bands_today = [b for b in schedule if b['date'] == current_date]
    logger.debug(f"Found {len(bands_today)} bands scheduled for today ({current_date})")

    # Zuerst prüfen ob eine Band gerade spielt
    for band in schedule:
        if band['date'] == current_date:
            band_time = datetime.strptime(band['start'], '%H:%M').time()
            band_datetime = datetime.combine(now.date(), band_time)
            band_end = band_datetime + timedelta(minutes=band['duration'])

            logger.debug(f"Checking band: {band['band']} at {band_datetime}")

            if band_datetime <= now <= band_end:
                logger.info(f"Found currently playing band: {band['band']}")
                return band, band_datetime

    # Wenn keine Band spielt, finde die nächste
    next_band = None
    next_time = None

    for band in schedule:
        if band['date'] == current_date:
            band_time = datetime.strptime(band['start'], '%H:%M').time()
            band_datetime = datetime.combine(now.date(), band_time)

            if band_datetime > now:
                if next_time is None or band_datetime < next_time:
                    next_band = band
                    next_time = band_datetime
                    logger.debug(f"Found next band: {band['band']} at {band_datetime}")

    if next_band:
        return next_band, next_time

    logger.info(f"No bands found for today ({current_date})")
    return None, None

def timer_thread():
    global timer_running, end_time, current_band_index, original_scheduled_start, original_scheduled_end, original_scheduled_duration
    last_date = None  # Track the last known date for date change detection
    loop_counter = 0  # Counter for periodic status logging

    while True:
        try:
            now = datetime.now()
            current_date = now.date().isoformat()
            loop_counter += 1

            # Detect date change (midnight transition)
            if last_date is not None and last_date != current_date:
                logger.info(f"Date changed from {last_date} to {current_date}")
                # Only reset if no band is currently playing (avoid interrupting cross-midnight performances)
                if not timer_running:
                    logger.info("No timer running - Resetting state for new day")
                    current_band_index = -1
                else:
                    logger.info(f"Timer still running - letting current band finish")

            last_date = current_date

            logger.debug("=== Timer Thread Check ===")
            logger.debug(f"Current time: {now}")
            logger.debug(f"Current date: {current_date}")
            logger.debug(f"Timer running: {timer_running}")
            if end_time:
                logger.debug(f"End time: {end_time}")

            # Wenn ein Timer läuft
            if timer_running and end_time:
                remaining = (end_time - now).total_seconds()
                if remaining > 0:
                    current_band = schedule[current_band_index]
                    logger.debug(f"Sending playing update for: {current_band['band']}")

                    # Finde die nächste Band (einfach die nächste im Schedule für heute)
                    next_band_info = None
                    for i in range(current_band_index + 1, len(schedule)):
                        next_band_candidate = schedule[i]
                        if next_band_candidate['date'] == current_date:  # Nur heute
                            next_band_time = datetime.strptime(next_band_candidate['start'], '%H:%M').time()
                            next_band_datetime = datetime.combine(now.date(), next_band_time)
                            next_band_info = {
                                "band": next_band_candidate['band'],
                                "start": next_band_candidate["start"],
                                "date": next_band_candidate["date"],
                                "countdown": (next_band_datetime - now).total_seconds()
                            }
                            break

                    update_data = {
                        'status': 'playing',
                        'band': current_band['band'],
                        'remaining': remaining,
                        'total_duration': current_band['duration'] * 60,
                        'warn_orange': get_warn_orange(),
                        'warn_red': get_warn_red()
                    }

                    # Füge Band-Logo hinzu, falls vorhanden
                    band_logo = db.get_band_logo(current_band['band'])
                    if band_logo:
                        update_data['band_logo'] = band_logo

                    if next_band_info:
                        update_data['next_band'] = next_band_info

                    socketio.emit('time_update', update_data)
                else:
                    # Timer ist abgelaufen - füge Band zur Historie hinzu
                    current_band = schedule[current_band_index]
                    logger.info(f"Timer ended for band: {current_band['band']}")

                    # Füge zur Historie hinzu
                    try:
                        # Tatsächliches Ende ist jetzt
                        actual_end = now
                        # Tatsächlicher Start war end_time minus tatsächliche duration
                        actual_start = end_time - timedelta(minutes=current_band['duration'])
                        # Tatsächliche Dauer in Minuten
                        actual_duration = int((actual_end - actual_start).total_seconds() / 60)

                        # Verwende die ursprünglich geplanten Zeiten für scheduled_start/end
                        db.add_to_history(
                            band_name=current_band['band'],
                            scheduled_date=current_band['date'],
                            scheduled_start=original_scheduled_start or current_band['start'],
                            scheduled_end=original_scheduled_end or current_band['end'],
                            actual_start=actual_start.isoformat(),
                            actual_end=actual_end.isoformat(),
                            duration=actual_duration
                        )
                        logger.info(f"Added '{current_band['band']}' to history")

                        # Entferne die Band aus dem Schedule
                        band_name_to_remove = current_band['band']
                        schedule.pop(current_band_index)
                        save_schedule_to_db()
                        logger.info(f"Removed '{band_name_to_remove}' from schedule")

                        # Sende Update an Frontend dass Schedule aktualisiert wurde
                        socketio.emit('schedule_updated', {'message': 'Band removed from schedule'})
                    except Exception as e:
                        logger.error(f"Failed to add band to history: {e}")

                    timer_running = False
                    end_time = None
                    current_band_index = -1

            # Wenn kein Timer läuft
            if not timer_running:
                next_band, next_time = find_next_band()

                if next_band:
                    if next_time <= now:  # Band sollte jetzt spielen
                        logger.info(f"Starting timer for current band: {next_band['band']}")
                        # Finde den Index der Band
                        for i, band in enumerate(schedule):
                            if band == next_band:
                                current_band_index = i
                                break
                        start_timer()
                    else:  # Band spielt später
                        time_until_start = (next_time - now).total_seconds()
                        logger.debug(f"Sending next band update: {next_band['band']}")
                        socketio.emit('time_update', {
                            'status': 'waiting',
                            'next_band': {
                                'band': next_band['band'],
                                'start': next_band['start'],
                                'date': next_band['date'],
                                'countdown': time_until_start
                            }
                        })
                else:
                    logger.debug("No bands found, sending finished status")
                    socketio.emit('time_update', {
                        'status': 'finished'
                    })

        except Exception as e:
            logger.error(f"Fehler im Timer Thread: {e}")

        # Periodic status logging every 1 hour (3600 seconds) - only when idle
        # This helps diagnose issues during long inactivity without spamming logs
        if loop_counter % 3600 == 0 and not timer_running:
            next_band, next_time = find_next_band()
            if next_band:
                hours_until = int((next_time - now).total_seconds() / 3600)
                logger.info(f"Idle - Next: {next_band['band']} in ~{hours_until}h ({next_band['start']})")
            else:
                logger.debug(f"Idle - No bands today ({current_date})")

        time.sleep(1)

def start_timer():
    global timer_running, end_time, original_scheduled_start, original_scheduled_end, original_scheduled_duration
    if current_band_index >= len(schedule):
        return

    band = schedule[current_band_index]
    now = datetime.now()
    band_time = datetime.strptime(band['start'], '%H:%M').time()
    band_start = datetime.combine(now.date(), band_time)

    # Wenn die Startzeit noch nicht erreicht ist, nicht starten
    if now < band_start:
        timer_running = False
        end_time = None
        return

    # Speichere die ursprünglich geplanten Zeiten für die Historie
    original_scheduled_start = band['start']
    original_scheduled_end = band['end']
    original_scheduled_duration = band['duration']

    # Berechne die verbleibende Zeit
    # Verwende die tatsächliche Startzeit (jetzt, falls verspätet; sonst geplant)
    actual_start = max(now, band_start)
    end_time = actual_start + timedelta(minutes=band['duration'])
    timer_running = True

    # Sende Update ans Frontend
    remaining = (end_time - now).total_seconds()
    logger.info(f"Starting timer for: {band['band']}")

    # Finde die nächste Band (einfach die nächste im Schedule für heute)
    current_date = now.date().isoformat()
    next_band_info = None
    for i in range(current_band_index + 1, len(schedule)):
        next_band_candidate = schedule[i]
        if next_band_candidate['date'] == current_date:  # Nur heute
            next_band_time = datetime.strptime(next_band_candidate['start'], '%H:%M').time()
            next_band_datetime = datetime.combine(now.date(), next_band_time)
            next_band_info = {
                "band": next_band_candidate['band'],
                "start": next_band_candidate["start"],
                "date": next_band_candidate["date"],
                "countdown": (next_band_datetime - now).total_seconds()
            }
            break

    update_data = {
        'status': 'playing',
        'band': band['band'],
        'remaining': remaining,
        'total_duration': band['duration'] * 60,
        'warn_orange': get_warn_orange(),
        'warn_red': get_warn_red()
    }

    if next_band_info:
        update_data['next_band'] = next_band_info

    socketio.emit('time_update', update_data)

def check_time_conflict(start_date, start_time, end_time, duration, end_date):
    """
    Überprüft, ob es Zeitkonflikte mit anderen Bands gibt.
    Berücksichtigt auch Bands, die über Mitternacht spielen.
    Returns: (bool, str) - (Hat Konflikt?, Fehlermeldung)
    """
    new_start = datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
    new_end = datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")

    for band in schedule:
        # Verwende end_date aus der Band-Daten (könnte am nächsten Tag sein)
        band_start = datetime.strptime(f"{band['date']} {band['start']}", "%Y-%m-%d %H:%M")
        band_end = datetime.strptime(f"{band['end_date']} {band['end']}", "%Y-%m-%d %H:%M")

        # Prüfe ob es eine Überschneidung gibt
        if (new_start < band_end and new_end > band_start):
            # Formatiere die Endzeit für die Fehlermeldung
            band_end_time = f"{band['end']} Uhr"
            if band['end_date'] != band['date']:
                band_end_time += f" (am {band['end_date']})"
            return True, f"Zeitkonflikt: {band['band']} spielt zu dieser Zeit noch bis {band_end_time}"

    return False, ""

@app.route('/')
@app.route('/stage')
@login_required
def stage():
    if not current_user.can_access_stage():
        abort(403)
    return render_template('index.html', logo=get_logo_filename(), logo_size_percent=get_logo_size_percent(), band_logos=get_band_logos())

@app.route('/backstage')
@login_required
def backstage():
    if not current_user.can_access_backstage():
        abort(403)
    return render_template('backstage.html', logo=get_logo_filename(), logo_size_percent=get_logo_size_percent(), band_logos=get_band_logos())

@app.route('/timetable')
@login_required
def timetable():
    if not current_user.can_access_timetable():
        abort(403)
    return render_template('timetable.html', logo=get_logo_filename(), logo_size_percent=get_logo_size_percent())

@app.route('/guide')
@login_required
def guide():
    """Zeigt die Anleitung/Hilfeseite an"""
    return render_template('guide.html', logo=get_logo_filename(), logo_size_percent=get_logo_size_percent())

@app.route('/api/schedule')
def api_schedule():
    """API endpoint to get the complete schedule with day calculation (day starts at 02:00)"""
    now = datetime.now()

    # Calculate "event day" - day switches at 02:00, not midnight
    if now.hour < 2:
        # Between 00:00-01:59 belongs to previous day
        event_date = (now.date() - timedelta(days=1)).isoformat()
    else:
        event_date = now.date().isoformat()

    # Get all unique dates in schedule
    all_dates = sorted(set(band['date'] for band in schedule))

    return jsonify({
        'schedule': schedule,
        'event_date': event_date,
        'all_dates': all_dates
    })

@app.route('/status')
def status():
    now = datetime.now()
    current_date = now.date().isoformat()  # Format YYYY-MM-DD

    logger.debug("=== Status Check ===")
    logger.debug(f"Aktuelle Zeit: {now}")
    logger.debug(f"Aktuelles Datum: {current_date}")

    # Prüfe ob es überhaupt Bands gibt
    if not schedule:
        logger.info("Schedule ist leer")
        return jsonify({"status": "finished"})

    logger.debug("Alle Bands im Schedule:")
    for band in schedule:
        logger.debug(f"Band: {band['band']}, Datum: {band['date']}, Start: {band['start']}")
    
    # Prüfe zuerst, ob eine Band gerade spielt
    for band in schedule:
        # Vergleiche Datum im gleichen Format (YYYY-MM-DD)
        band_date = band['date']
        if band_date == current_date:  # Nur für heute
            band_time = datetime.strptime(band['start'], '%H:%M').time()
            band_datetime = datetime.combine(now.date(), band_time)
            band_end_datetime = band_datetime + timedelta(minutes=band['duration'])
            
            logger.debug(f"Prüfe aktuelle Band: {band['band']}")
            logger.debug(f"Band Datum: {band_date}")
            logger.debug(f"Aktuelles Datum: {current_date}")
            logger.debug(f"Start: {band_datetime}")
            logger.debug(f"Ende: {band_end_datetime}")
            logger.debug(f"Jetzt: {now}")

            # Wenn die Band jetzt spielt
            if band_datetime <= now <= band_end_datetime:
                logger.info(f"Band spielt gerade: {band['band']}")
                remaining_time = (band_end_datetime - now).total_seconds()

                # Finde die nächste Band (einfach die nächste im Schedule für heute)
                next_band_info = None
                current_band_index = schedule.index(band)
                for i in range(current_band_index + 1, len(schedule)):
                    next_band = schedule[i]
                    if next_band['date'] == current_date:  # Nur heute
                        next_band_time = datetime.strptime(next_band['start'], '%H:%M').time()
                        next_band_datetime = datetime.combine(now.date(), next_band_time)
                        next_band_info = {
                            "band": next_band['band'],
                            "start": next_band["start"],
                            "date": next_band["date"],
                            "countdown": (next_band_datetime - now).total_seconds()
                        }
                        logger.info(f"Nächste Band nach aktueller: {next_band['band']}")
                        break

                response = {
                    "status": "playing",
                    "band": band['band'],
                    "remaining": remaining_time,
                    "total_duration": band['duration'] * 60,
                    "warn_orange": get_warn_orange(),
                    "warn_red": get_warn_red()
                }

                # Füge Band-Logo hinzu, falls vorhanden
                band_logo = db.get_band_logo(band['band'])
                if band_logo:
                    response["band_logo"] = band_logo

                if next_band_info:
                    response["next_band"] = next_band_info

                return jsonify(response)
    
    # Wenn keine Band spielt, finde die nächste Band
    next_possible_band = None
    for band in schedule:
        if band['date'] == current_date:  # Nur für heute
            band_time = datetime.strptime(band['start'], '%H:%M').time()
            band_datetime = datetime.combine(now.date(), band_time)
            
            logger.debug(f"Prüfe nächste Band: {band['band']}")
            logger.debug(f"Band DateTime: {band_datetime}")
            logger.debug(f"Ist in Zukunft: {band_datetime > now}")

            # Wenn die Band in der Zukunft spielt
            if band_datetime > now:
                if not next_possible_band or band_datetime < next_possible_band[1]:
                    next_possible_band = (band, band_datetime)
                    logger.debug(f"Nächste Band gefunden/aktualisiert: {band['band']}")
    
    # Wenn es eine nächste Band gibt
    if next_possible_band:
        next_band, next_datetime = next_possible_band
        logger.info(f"Nächste Band: {next_band['band']} am {next_datetime}")

        time_until_start = (next_datetime - now).total_seconds()
        return jsonify({
            "status": "waiting",
            "next_band": {
                "band": next_band['band'],
                "start": next_band["start"],
                "date": next_band["date"],
                "countdown": time_until_start
            }
        })

    # Wenn keine Band spielt und keine nächste Band heute spielt
    logger.info("Keine aktive oder nächste Band gefunden")
    return jsonify({"status": "finished"})

@app.route('/admin', methods=['GET', 'POST'])
@login_required
def admin():
    # Nur Stagemanager und Admin haben Zugriff
    if not current_user.can_access_admin():
        abort(403)

    global current_band_index, timer_running, end_time
    today = datetime.now().date().isoformat()
    is_full_admin = current_user.is_admin()

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'start':
            current_band_index = 0
            start_timer()
        elif action == 'pause':
            timer_running = False
        elif action == 'upload_logo':
            if not is_full_admin:
                abort(403)
            file = request.files.get('logo')
            if file and file.filename:
                # Validiere Dateityp
                allowed_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.svg'}
                file_ext = os.path.splitext(file.filename)[1].lower()
                if file_ext not in allowed_extensions:
                    logger.warning(f"Ungültiger Dateityp: {file_ext}")
                    return "Ungültiger Dateityp. Nur Bilder erlaubt.", 400

                # Erstelle Upload-Verzeichnis falls nicht vorhanden
                os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

                # Speichere Datei
                try:
                    path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
                    file.save(path)
                    db.set_setting('logo_filename', file.filename)
                    logger.info(f"Logo hochgeladen: {file.filename}")
                except Exception as e:
                    logger.error(f"Fehler beim Hochladen des Logos: {e}")
                    return "Fehler beim Hochladen", 500
        elif action == 'set_logo_size':
            if not is_full_admin:
                abort(403)
            logo_size_percent = int(request.form.get('logo_size_percent', 10))
            db.set_setting('logo_size_percent', str(logo_size_percent))
        elif action == 'reload':
            if not is_full_admin:
                abort(403)
            load_schedule()
        elif action == 'save':
            # Speichere alten Schedule für Smart Rename
            old_schedule = {f"{band['date']}_{band['start']}": band['band'] for band in schedule}

            # Sammle alle Bands (bestehende und neue)
            updated_schedule = []
            for key in request.form:
                if key.startswith('date_'):
                    index = key.split('_')[1]
                    date = request.form.get(f'date_{index}')
                    band = request.form.get(f'band_{index}')
                    start = request.form.get(f'start_{index}')
                    end = request.form.get(f'end_{index}')

                    if all([date, band, start, end]):  # Prüfe ob alle Felder ausgefüllt sind
                        # Berechne Dauer und End-Datum
                        duration, end_date = calculate_duration_and_end_date(date, start, end)

                        updated_schedule.append({
                            "date": date,
                            "band": band,
                            "start": start,
                            "end": end,
                            "end_date": end_date,
                            "duration": duration
                        })

            # Sortiere nach Datum und Zeit
            updated_schedule.sort(key=lambda x: (x['date'], x['start']))

            # Smart Rename: Band-Logo-Zuordnungen aktualisieren
            for new_band in updated_schedule:
                key = f"{new_band['date']}_{new_band['start']}"
                old_name = old_schedule.get(key)
                new_name = new_band['band']

                # Wenn sich der Name geändert hat und das alte Band ein Logo hatte
                if old_name and old_name != new_name and db.get_band_logo(old_name):
                    # Verschiebe Logo-Zuordnung zum neuen Namen in der DB
                    db.rename_band_in_logos(old_name, new_name)
                    logger.info(f"Smart Rename: '{old_name}' -> '{new_name}' (Logo übertragen)")

            # Aktualisiere den globalen Schedule
            schedule.clear()
            schedule.extend(updated_schedule)
            save_schedule_to_db()

            # Wenn ein Timer läuft, aktualisiere die end_time basierend auf dem neuen Schedule
            if timer_running and current_band_index >= 0 and current_band_index < len(schedule):
                current_band = schedule[current_band_index]
                # Berechne die neue Endzeit basierend auf Start + Dauer
                start_datetime = datetime.strptime(f"{current_band['date']} {current_band['start']}", "%Y-%m-%d %H:%M")
                end_time = start_datetime + timedelta(minutes=current_band['duration'])
                logger.info(f"Timer end_time aktualisiert für '{current_band['band']}': {end_time}")
            
        elif action == 'add_band':
            date = request.form.get('date')
            band = request.form.get('band')
            start = request.form.get('start')
            end = request.form.get('end')

            if all([date, band, start, end]):
                # Berechne Dauer und End-Datum
                duration, end_date = calculate_duration_and_end_date(date, start, end)

                # Prüfe auf Zeitkonflikte
                has_conflict, error_message = check_time_conflict(date, start, end, duration, end_date)
                if has_conflict:
                    return error_message, 400

                # Füge neue Band hinzu
                schedule.append({
                    "date": date,
                    "band": band,
                    "start": start,
                    "end": end,
                    "end_date": end_date,
                    "duration": duration
                })

                # Sortiere nach Datum und Zeit
                schedule.sort(key=lambda x: (x['date'], x['start']))
                save_schedule_to_db()
            
        elif action == 'delete':
            selected = request.form.getlist('selected[]')
            # Konvertiere "new_X" Indizes in numerische Werte
            indices = []
            for idx in selected:
                if idx.startswith('new_'):
                    continue  # Überspringe neue (noch nicht gespeicherte) Einträge
                indices.append(int(idx))
            
            # Sortiere die Indizes absteigend, um Probleme beim Löschen zu vermeiden
            indices.sort(reverse=True)
            
            # Lösche die ausgewählten Einträge
            for idx in indices:
                if 0 <= idx < len(schedule):
                    schedule.pop(idx)

            save_schedule_to_db()

            # Prüfe ob die gerade spielende Band gelöscht wurde
            if timer_running and current_band_index >= len(schedule):
                # Die spielende Band wurde gelöscht - stoppe den Timer
                timer_running = False
                end_time = None
                current_band_index = -1
                logger.info("Currently playing band was deleted - timer stopped")

        elif action == 'update_config':
            if not is_full_admin:
                abort(403)
            try:
                warn_orange = int(request.form.get('warn_orange', 5))
                warn_red = int(request.form.get('warn_red', 1))

                # Validierung
                if warn_orange <= 0 or warn_red <= 0:
                    return "Warnzeiten müssen positiv sein", 400
                if warn_red >= warn_orange:
                    return "Rote Warnung muss kleiner als orange Warnung sein", 400

                # Speichere in DB
                db.set_setting('warn_orange', str(warn_orange))
                db.set_setting('warn_red', str(warn_red))
                logger.info(f"Konfiguration aktualisiert: orange={warn_orange}, red={warn_red}")
            except ValueError:
                return "Ungültige Warnzeit-Werte", 400
        elif action == 'add_user':
            if not is_full_admin:
                abort(403)
            new_user = request.form.get('new_username', '').strip()
            new_pass = request.form.get('new_password', '')

            # Validierung
            if not new_user or not new_pass:
                return "Benutzername und Passwort erforderlich", 400
            if len(new_user) < 3:
                return "Benutzername muss mindestens 3 Zeichen lang sein", 400
            if len(new_pass) < 6:
                return "Passwort muss mindestens 6 Zeichen lang sein", 400

            # Prüfe ob Benutzer bereits existiert
            if db.get_user(new_user):
                return "Benutzername existiert bereits", 400

            try:
                password_hash = generate_password_hash(new_pass)
                db.add_user(new_user, password_hash)
                logger.info(f"Neuer Benutzer erstellt: {new_user}")
            except Exception as e:
                logger.error(f"Fehler beim Erstellen des Benutzers: {e}")
                return "Fehler beim Erstellen des Benutzers", 500
        elif action == 'delete_user':
            if not is_full_admin:
                abort(403)
            username = request.form.get('username')
            # Verhindere das Löschen des letzten Admin-Users
            if db.count_admins() == 1 and db.user_has_role(username, 'Admin'):
                return "Letzter Admin kann nicht gelöscht werden", 400
            # Verhindere das Löschen des eigenen Users
            if username == current_user.id:
                return "Eigenen Account kann nicht gelöscht werden", 400
            try:
                db.delete_user(username)
                logger.info(f"Benutzer gelöscht: {username}")
            except Exception as e:
                logger.error(f"Fehler beim Löschen des Benutzers: {e}")
        elif action == 'adjust_time':
            # Quick-Adjust: Verlängere/verkürze die laufende Band um X Minuten
            adjust_minutes = int(request.form.get('adjust_minutes', 0))

            if timer_running and current_band_index >= 0 and adjust_minutes != 0:
                current_band = schedule[current_band_index]
                current_date = current_band['date']

                # Prüfe bei negativen Werten ob genug Zeit übrig ist
                if adjust_minutes < 0:
                    new_duration = current_band['duration'] + adjust_minutes
                    if new_duration < 1:
                        return jsonify({"success": False, "message": "Duration muss mindestens 1 Minute sein"}), 400

                # Bei positiven Werten: Prüfe ob die Verlängerung die nächste Band überlappt
                if adjust_minutes > 0:
                    # Finde die nächste Band am gleichen Tag
                    next_band = None
                    for i in range(current_band_index + 1, len(schedule)):
                        if schedule[i]['date'] == current_date:
                            next_band = schedule[i]
                            break
                        elif schedule[i]['date'] > current_date:
                            break

                    if next_band:
                        # Berechne das neue Ende der aktuellen Band
                        current_start = datetime.strptime(f"{current_band['date']} {current_band['start']}", "%Y-%m-%d %H:%M")
                        new_end = current_start + timedelta(minutes=current_band['duration'] + adjust_minutes)

                        # Berechne den Start der nächsten Band
                        next_start = datetime.strptime(f"{next_band['date']} {next_band['start']}", "%Y-%m-%d %H:%M")

                        # Prüfe ob es eine Überlappung gibt
                        if new_end > next_start:
                            # Berechne maximale mögliche Verlängerung
                            max_possible = int((next_start - current_start).total_seconds() / 60) - current_band['duration']
                            if max_possible > 0:
                                return jsonify({"success": False, "message": f"Nur +{max_possible} Min möglich (nächste Band um {next_band['start']} Uhr)"}), 400
                            else:
                                return jsonify({"success": False, "message": f"Keine Verlängerung möglich (nächste Band um {next_band['start']} Uhr)"}), 400

                # Verlängere end_time
                end_time = end_time + timedelta(minutes=adjust_minutes)

                # Verlängere/verkürze duration im schedule
                schedule[current_band_index]['duration'] += adjust_minutes

                # Berechne neue End-Zeit aus der neuen Duration
                new_start_datetime = datetime.strptime(f"{current_band['date']} {current_band['start']}", "%Y-%m-%d %H:%M")
                new_end_datetime = new_start_datetime + timedelta(minutes=schedule[current_band_index]['duration'])
                schedule[current_band_index]['end'] = new_end_datetime.strftime("%H:%M")
                schedule[current_band_index]['end_date'] = new_end_datetime.date().isoformat()

                # Speichere den aktualisierten Schedule
                save_schedule_to_db()

                action_text = "verlängert" if adjust_minutes > 0 else "verkürzt"
                logger.info(f"Band '{current_band['band']}' um {abs(adjust_minutes)} Min {action_text}")

                # Sende sofortiges Update ans Frontend
                now = datetime.now()
                remaining = (end_time - now).total_seconds()

                # Finde die nächste Band (einfach die nächste im Schedule für heute)
                next_band_info = None
                for i in range(current_band_index + 1, len(schedule)):
                    next_band_candidate = schedule[i]
                    if next_band_candidate['date'] == current_date:  # Nur heute
                        next_band_time = datetime.strptime(next_band_candidate['start'], '%H:%M').time()
                        next_band_datetime = datetime.combine(now.date(), next_band_time)
                        next_band_info = {
                            "band": next_band_candidate['band'],
                            "start": next_band_candidate["start"],
                            "date": next_band_candidate["date"],
                            "countdown": (next_band_datetime - now).total_seconds()
                        }
                        break

                update_data = {
                    'status': 'playing',
                    'band': schedule[current_band_index]['band'],
                    'remaining': remaining,
                    'total_duration': schedule[current_band_index]['duration'] * 60,
                    'warn_orange': get_warn_orange(),
                    'warn_red': get_warn_red()
                }

                if next_band_info:
                    update_data['next_band'] = next_band_info

                socketio.emit('time_update', update_data)

                message_prefix = "+" if adjust_minutes > 0 else ""
                return jsonify({"success": True, "message": f"{message_prefix}{adjust_minutes} Min"})

            return jsonify({"success": False, "message": "Kein Timer läuft"}), 400

        return redirect(url_for('admin'))

    # Lade Benutzerliste mit Rollen aus DB (nur für Admin)
    userlist = db.get_users_with_roles() if is_full_admin else []
    all_roles = db.get_all_roles() if is_full_admin else []

    # Debug: Log schedule_conflicts vor dem Rendern
    logger.debug(f"=== Rendering Admin Page ===")
    logger.debug(f"schedule_conflicts hat {len(schedule_conflicts)} Einträge: {schedule_conflicts}")

    return render_template(
        'admin.html',
        schedule=schedule,
        logo=get_logo_filename(),
        logo_size_percent=get_logo_size_percent(),
        users=userlist,
        all_roles=all_roles,
        warn_orange=get_warn_orange(),
        warn_red=get_warn_red(),
        today=today,
        schedule_conflicts=schedule_conflicts,
        band_logos=get_band_logos(),
        is_full_admin=is_full_admin,
        event_password_enabled=db.is_event_password_enabled()
    )

@app.route('/download_example_csv')
@login_required
def download_example_csv():
    """Download einer korrekt formatierten Beispiel-CSV"""
    # Verwende morgen als Datum, damit die Beispieldaten immer in der Zukunft liegen
    tomorrow = datetime.now().date() + timedelta(days=1)
    day_after = tomorrow + timedelta(days=1)

    tomorrow_str = tomorrow.isoformat()
    day_after_str = day_after.isoformat()

    # Erstelle Beispiel-CSV-Daten (immer ab morgen)
    example_data = f"""date,band,start,end
{tomorrow_str},Band 1,18:00,19:00
{tomorrow_str},Band 2,19:30,21:00
{tomorrow_str},Band 3,21:30,23:00
{day_after_str},Band 4,14:00,15:30
{day_after_str},Band 5,16:00,17:30"""

    # Erstelle einen BytesIO-Stream
    buffer = io.BytesIO()
    buffer.write(example_data.encode('utf-8'))
    buffer.seek(0)

    return send_file(
        buffer,
        mimetype='text/csv',
        as_attachment=True,
        download_name='schedule_example.csv'
    )

@app.route('/upload_csv', methods=['POST'])
@login_required
def upload_csv():
    """Upload einer CSV-Datei und Validierung"""
    global schedule_conflicts

    if 'csv_file' not in request.files:
        return jsonify({"success": False, "message": "Keine Datei ausgewählt"}), 400

    file = request.files['csv_file']

    if file.filename == '':
        return jsonify({"success": False, "message": "Keine Datei ausgewählt"}), 400

    # Prüfe ob es eine CSV-Datei ist
    if not file.filename.lower().endswith('.csv'):
        return jsonify({"success": False, "message": "Nur CSV-Dateien erlaubt"}), 400

    try:
        # Lese die CSV-Datei direkt aus dem Upload
        df = pd.read_csv(file)

        # Validiere die Spalten
        required_columns = ['date', 'band', 'start', 'end']
        if not all(col in df.columns for col in required_columns):
            return jsonify({
                "success": False,
                "message": f"CSV muss die Spalten enthalten: {', '.join(required_columns)}"
            }), 400

        # Prüfe ob die CSV leer ist
        if len(df) == 0:
            return jsonify({"success": False, "message": "CSV-Datei ist leer"}), 400

        # Entferne Leerzeichen aus Band-Namen
        df['band'] = df['band'].str.strip()

        # Sortiere nach Datum und Startzeit
        df = df.sort_values(['date', 'start'])

        # Validiere und prüfe auf Konflikte (wie in load_schedule)
        temp_schedule = []
        conflicts_found = []

        for idx, row in df.iterrows():
            try:
                # Berechne Dauer und End-Datum
                duration, end_date = calculate_duration_and_end_date(
                    row["date"],
                    row["start"],
                    row["end"]
                )

                # Erstelle temporären Eintrag
                temp_entry = {
                    "date": row["date"],
                    "band": row["band"],
                    "start": row["start"],
                    "end": row["end"],
                    "end_date": end_date,
                    "duration": duration
                }

                # Prüfe auf Zeitkonflikte
                new_start = datetime.strptime(f"{row['date']} {row['start']}", "%Y-%m-%d %H:%M")
                new_end = datetime.strptime(f"{end_date} {row['end']}", "%Y-%m-%d %H:%M")

                for other_band in temp_schedule:
                    other_start = datetime.strptime(f"{other_band['date']} {other_band['start']}", "%Y-%m-%d %H:%M")
                    other_end = datetime.strptime(f"{other_band['end_date']} {other_band['end']}", "%Y-%m-%d %H:%M")

                    # Prüfe auf Überschneidung
                    if (new_start < other_end and new_end > other_start):
                        conflict_msg = {
                            "row": idx + 2,
                            "band1": row['band'],
                            "time1": f"{row['start']}-{row['end']}",
                            "band2": other_band['band'],
                            "time2": f"{other_band['start']}-{other_band['end']}"
                        }
                        conflicts_found.append(conflict_msg)

                temp_schedule.append(temp_entry)

            except Exception as e:
                return jsonify({
                    "success": False,
                    "message": f"Fehler in Zeile {idx + 2}: {str(e)}"
                }), 400

        # Wenn Konflikte gefunden wurden, nicht speichern
        if conflicts_found:
            conflict_messages = []
            for c in conflicts_found:
                conflict_messages.append(
                    f"Zeile {c['row']}: '{c['band1']}' ({c['time1']}) überschneidet sich mit '{c['band2']}' ({c['time2']})"
                )
            return jsonify({
                "success": False,
                "message": f"{len(conflicts_found)} Zeitkonflikt(e) gefunden:\n" + "\n".join(conflict_messages)
            }), 400

        # Alles OK - importiere in Datenbank
        db.import_bands_from_list(temp_schedule)

        # Lade den Schedule neu aus der DB
        load_schedule()

        return jsonify({
            "success": True,
            "message": f"CSV erfolgreich hochgeladen! {len(temp_schedule)} Band(s) geladen."
        })

    except Exception as e:
        logger.error(f"Fehler beim CSV-Upload: {e}")
        return jsonify({"success": False, "message": f"Fehler beim Verarbeiten der CSV: {str(e)}"}), 400

@app.route('/upload_band_logo', methods=['POST'])
@login_required
def upload_band_logo():
    """Upload eines Band-Logos"""
    if 'logo_file' not in request.files:
        return jsonify({"success": False, "message": "Keine Datei ausgewählt"}), 400

    file = request.files['logo_file']
    band_name = request.form.get('band_name', '').strip()

    if not band_name:
        return jsonify({"success": False, "message": "Band-Name fehlt"}), 400

    if file.filename == '':
        return jsonify({"success": False, "message": "Keine Datei ausgewählt"}), 400

    # Prüfe Dateityp
    allowed_extensions = {'.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp'}
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in allowed_extensions:
        return jsonify({"success": False, "message": "Nur Bild-Dateien erlaubt (PNG, JPG, GIF, SVG, WEBP)"}), 400

    try:
        # Lösche altes Logo, falls vorhanden
        old_logo = db.get_band_logo(band_name)
        if old_logo:
            old_logo_path = os.path.join('static/uploads/band_logos', old_logo)
            if os.path.exists(old_logo_path):
                os.remove(old_logo_path)

        # Erstelle sicheren Dateinamen mit Timestamp
        timestamp = int(time.time())
        safe_band_name = secure_filename(band_name)
        filename = f"{safe_band_name}_{timestamp}{file_ext}"
        filepath = os.path.join('static/uploads/band_logos', filename)

        # Speichere Datei
        file.save(filepath)

        # Aktualisiere Mapping in DB
        db.set_band_logo(band_name, filename)

        return jsonify({
            "success": True,
            "message": "Logo erfolgreich hochgeladen",
            "filename": filename
        })

    except Exception as e:
        logger.error(f"Fehler beim Band-Logo-Upload: {e}")
        return jsonify({"success": False, "message": f"Fehler beim Hochladen: {str(e)}"}), 400

@app.route('/delete_band_logo', methods=['POST'])
@login_required
def delete_band_logo():
    """Löscht ein Band-Logo"""
    band_name = request.form.get('band_name', '').strip()

    if not band_name:
        return jsonify({"success": False, "message": "Band-Name fehlt"}), 400

    logo_filename = db.get_band_logo(band_name)
    if not logo_filename:
        return jsonify({"success": False, "message": "Kein Logo für diese Band vorhanden"}), 400

    try:
        # Lösche Datei
        filepath = os.path.join('static/uploads/band_logos', logo_filename)
        if os.path.exists(filepath):
            os.remove(filepath)

        # Entferne aus DB
        db.delete_band_logo(band_name)

        return jsonify({
            "success": True,
            "message": "Logo erfolgreich gelöscht"
        })

    except Exception as e:
        logger.error(f"Fehler beim Löschen des Band-Logos: {e}")
        return jsonify({"success": False, "message": f"Fehler beim Löschen: {str(e)}"}), 400

# ==================== HISTORIE ====================

@app.route('/api/history', methods=['GET'])
@login_required
def get_history():
    """Gibt die sichtbare Historie zurück"""
    try:
        history_entries = db.get_visible_history(limit=100)
        return jsonify({
            "success": True,
            "history": history_entries
        })
    except Exception as e:
        logger.error(f"Fehler beim Laden der Historie: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/history/hide', methods=['POST'])
@login_required
def hide_history_entry():
    """Versteckt einen einzelnen Historie-Eintrag (soft delete)"""
    try:
        history_id = request.form.get('history_id')
        if not history_id:
            return jsonify({"success": False, "message": "Historie-ID fehlt"}), 400

        db.hide_history_entry(int(history_id))
        logger.info(f"Historie-Eintrag {history_id} versteckt")

        return jsonify({
            "success": True,
            "message": "Eintrag wurde aus der Historie entfernt"
        })
    except Exception as e:
        logger.error(f"Fehler beim Verstecken des Historie-Eintrags: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/history/hide_all', methods=['POST'])
@login_required
def hide_all_history():
    """Versteckt alle Historie-Einträge (soft delete)"""
    try:
        db.hide_all_history()
        logger.info("Gesamte Historie versteckt")

        return jsonify({
            "success": True,
            "message": "Gesamte Historie wurde geleert"
        })
    except Exception as e:
        logger.error(f"Fehler beim Leeren der Historie: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


# ==================== ROLLEN-VERWALTUNG API ====================

@app.route('/api/user/<username>/roles', methods=['GET'])
@admin_required
def get_user_roles_api(username):
    """Gibt die Rollen eines Benutzers zurück"""
    user = db.get_user(username)
    if not user:
        return jsonify({'success': False, 'message': 'Benutzer nicht gefunden'}), 404

    roles = db.get_user_roles(username)
    all_roles = db.get_all_roles()

    return jsonify({
        'success': True,
        'username': username,
        'roles': roles,
        'all_roles': [r['name'] for r in all_roles]
    })


@app.route('/api/user/<username>/roles', methods=['POST'])
@admin_required
def set_user_roles_api(username):
    """Setzt die Rollen eines Benutzers"""
    user = db.get_user(username)
    if not user:
        return jsonify({'success': False, 'message': 'Benutzer nicht gefunden'}), 404

    data = request.get_json()
    roles = data.get('roles', [])

    # Validiere Rollen-Kombination
    if not db.validate_role_combination(roles):
        return jsonify({
            'success': False,
            'message': 'Ungültige Rollen-Kombination. Viewer-Rollen können kombiniert werden, Stagemanager und Admin sind exklusiv.'
        }), 400

    # Verhindere das Entfernen der eigenen Admin-Rolle
    if username == current_user.id and 'Admin' not in roles and current_user.is_admin():
        return jsonify({
            'success': False,
            'message': 'Eigene Admin-Rolle kann nicht entfernt werden'
        }), 400

    # Verhindere das Entfernen des letzten Admins
    if 'Admin' not in roles and db.user_has_role(username, 'Admin') and db.count_admins() == 1:
        return jsonify({
            'success': False,
            'message': 'Letzter Admin-Benutzer kann nicht degradiert werden'
        }), 400

    db.set_user_roles(user['id'], roles)
    logger.info(f"Rollen für '{username}' aktualisiert: {roles}")

    return jsonify({
        'success': True,
        'message': 'Rollen aktualisiert',
        'roles': roles
    })


@app.route('/api/roles', methods=['GET'])
@admin_required
def get_all_roles_api():
    """Gibt alle verfügbaren Rollen zurück"""
    roles = db.get_all_roles()
    return jsonify({
        'success': True,
        'roles': roles
    })


# ==================== PASSWORT-VERWALTUNG API ====================

@app.route('/api/user/change-password', methods=['POST'])
@login_required
def change_own_password():
    """Ermöglicht einem eingeloggten Benutzer sein eigenes Passwort zu ändern"""
    # Event-User dürfen kein Passwort ändern
    if current_user.is_event_user:
        return jsonify({
            'success': False,
            'message': 'Event-Benutzer können kein Passwort ändern'
        }), 403

    data = request.get_json()
    current_password = data.get('current_password', '')
    new_password = data.get('new_password', '')

    # Validierung
    if not current_password or not new_password:
        return jsonify({
            'success': False,
            'message': 'Aktuelles und neues Passwort erforderlich'
        }), 400

    if len(new_password) < 6:
        return jsonify({
            'success': False,
            'message': 'Neues Passwort muss mindestens 6 Zeichen lang sein'
        }), 400

    # Aktuelles Passwort überprüfen
    user = db.get_user(current_user.id)
    if not user or not check_password_hash(user['password_hash'], current_password):
        return jsonify({
            'success': False,
            'message': 'Aktuelles Passwort ist falsch'
        }), 400

    # Neues Passwort setzen
    new_hash = generate_password_hash(new_password)
    if db.update_user_password(current_user.id, new_hash):
        logger.info(f"Benutzer '{current_user.id}' hat sein Passwort geändert")
        return jsonify({
            'success': True,
            'message': 'Passwort erfolgreich geändert'
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Fehler beim Ändern des Passworts'
        }), 500


@app.route('/api/user/<username>/reset-password', methods=['POST'])
@admin_required
def reset_user_password(username):
    """Ermöglicht einem Admin das Passwort eines anderen Benutzers zurückzusetzen"""
    data = request.get_json()
    new_password = data.get('new_password', '')

    # Validierung
    if not new_password:
        return jsonify({
            'success': False,
            'message': 'Neues Passwort erforderlich'
        }), 400

    if len(new_password) < 6:
        return jsonify({
            'success': False,
            'message': 'Neues Passwort muss mindestens 6 Zeichen lang sein'
        }), 400

    # Prüfen ob User existiert
    user = db.get_user(username)
    if not user:
        return jsonify({
            'success': False,
            'message': 'Benutzer nicht gefunden'
        }), 404

    # Neues Passwort setzen
    new_hash = generate_password_hash(new_password)
    if db.update_user_password(username, new_hash):
        logger.info(f"Admin '{current_user.id}' hat das Passwort von '{username}' zurückgesetzt")
        return jsonify({
            'success': True,
            'message': f'Passwort für {username} wurde zurückgesetzt'
        })
    else:
        return jsonify({
            'success': False,
            'message': 'Fehler beim Zurücksetzen des Passworts'
        }), 500


# ==================== EVENT-PASSWORT API ====================

@app.route('/api/settings/event-password', methods=['POST'])
@admin_required
def set_event_password_api():
    """Setzt oder löscht das Veranstaltungspasswort"""
    data = request.get_json()
    password = data.get('password', '')

    if password:
        if len(password) < 4:
            return jsonify({
                'success': False,
                'message': 'Passwort muss mindestens 4 Zeichen lang sein'
            }), 400
        db.set_event_password(password)
        logger.info("Veranstaltungspasswort wurde gesetzt")
        return jsonify({
            'success': True,
            'message': 'Veranstaltungspasswort wurde gesetzt',
            'enabled': True
        })
    else:
        db.clear_event_password()
        logger.info("Veranstaltungspasswort wurde gelöscht")
        return jsonify({
            'success': True,
            'message': 'Veranstaltungspasswort wurde gelöscht',
            'enabled': False
        })


@app.route('/api/settings/event-password', methods=['GET'])
@admin_required
def get_event_password_status():
    """Gibt den Status des Veranstaltungspassworts zurück"""
    return jsonify({
        'success': True,
        'enabled': db.is_event_password_enabled()
    })


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        login_type = request.form.get('login_type', 'user')

        if login_type == 'event':
            # Login mit Veranstaltungspasswort
            event_password = request.form.get('event_password', '')
            if db.verify_event_password(event_password):
                event_user = EventUser()
                login_user(event_user)
                return redirect(url_for('stage'))
            return render_template('login.html',
                                   logo=get_logo_filename(),
                                   event_password_enabled=db.is_event_password_enabled(),
                                   error_event=True)
        else:
            # Regulärer Benutzer-Login
            username = request.form.get('username', '')
            password = request.form.get('password', '')
            user = db.get_user(username)

            if user and check_password_hash(user['password_hash'], password):
                roles = db.get_user_roles(username)

                # Prüfe ob User mindestens eine Rolle hat
                if not roles:
                    return render_template('login.html',
                                           logo=get_logo_filename(),
                                           event_password_enabled=db.is_event_password_enabled(),
                                           error_user=True,
                                           error_message='Keine Rolle zugewiesen. Kontaktiere einen Admin.')

                login_user(User(username, roles=roles))

                # Redirect basierend auf Rollen
                if 'Admin' in roles or 'Stagemanager' in roles:
                    return redirect(url_for('admin'))
                elif 'ViewerStage' in roles:
                    return redirect(url_for('stage'))
                elif 'ViewerBackstage' in roles:
                    return redirect(url_for('backstage'))
                elif 'ViewerTimetable' in roles:
                    return redirect(url_for('timetable'))
                else:
                    return redirect(url_for('stage'))

            return render_template('login.html',
                                   logo=get_logo_filename(),
                                   event_password_enabled=db.is_event_password_enabled(),
                                   error_user=True)

    # GET Request - Login-Seite anzeigen
    return render_template('login.html',
                           logo=get_logo_filename(),
                           event_password_enabled=db.is_event_password_enabled())

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

@socketio.on('admin_message')
def handle_admin_message(data):
    """Handle admin messages and broadcast to all connected clients"""
    message = data.get('message', '')
    duration = data.get('duration', 10)

    logger.info(f"Admin message: '{message}' for {duration} seconds")

    # Broadcast message to all connected clients
    socketio.emit('display_message', {
        'message': message,
        'duration': duration
    })

if __name__ == '__main__':
    # Erstelle notwendige Verzeichnisse
    os.makedirs('static/uploads/band_logos', exist_ok=True)

    # Log startup information
    logger.info("=" * 50)
    logger.info("StageTimer Application Starting")
    logger.info(f"Current time: {datetime.now()}")
    logger.info(f"Current date: {datetime.now().date().isoformat()}")
    logger.info(f"Timezone: {os.environ.get('TZ', 'Not set')}")
    logger.info("=" * 50)

    # Initialisiere Datenbank
    db.init_database()
    logger.info("Database initialized")

    # Lade Schedule aus DB
    load_schedule()

    # Log schedule information
    if schedule:
        dates = set(band['date'] for band in schedule)
        logger.info(f"Loaded {len(schedule)} bands across {len(dates)} days: {sorted(dates)}")
    else:
        logger.warning("No bands in schedule!")

    threading.Thread(target=timer_thread, daemon=True).start()

    # Produktionsmodus mit Gunicorn oder anderem WSGI-Server empfohlen
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)