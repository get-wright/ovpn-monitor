from flask import Flask, render_template, redirect, url_for, request, flash
import socket
import datetime
import threading
import time
from flask_socketio import SocketIO
<<<<<<< Updated upstream

app = Flask(__name__)
app.secret_key = 'secret!'
socketio = SocketIO(app)

# Configuration: list of OpenVPN management interface profiles.
# Each profile must include a unique name and its corresponding UNIX socket path.
profiles_config = [
    {"name": "profile1", "socket_path": "/run/openvpn/pt.sock"},
    # You can add more profiles here, for example:
    # {"name": "profile2", "socket_path": "/run/openvpn/profile2.sock"},
=======
import requests
import json
from collections import deque
import logging
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import desc, inspect, text
import os
from maxminddb import open_database
from flask_httpauth import HTTPBasicAuth
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)
app.secret_key = 'secret!'

# Authentication setup
auth = HTTPBasicAuth()

# Load credentials from environment variables
USERNAME = os.getenv('FLASK_USERNAME')
PASSWORD = os.getenv('FLASK_PASSWORD')

@auth.verify_password
def verify_password(username, password):
    if username == USERNAME and password == PASSWORD:
        return username
    return None

# Database configuration
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'ovpn_monitor.db')
os.makedirs(os.path.dirname(db_path), exist_ok=True)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

socketio = SocketIO(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('ovpn-monitor')

# Configuration: list of OpenVPN management interface profiles
profiles_config = [
    {"name": "Active BKCTF players", "socket_path": "/run/openvpn/pt.sock"},
>>>>>>> Stashed changes
]

profile_data = {}
profile_ip_log = {}

<<<<<<< Updated upstream
def update_profile_status():
    """
    Background thread function that periodically connects to each OpenVPN management interface,
    sends the "status" command, parses the output, and updates the client list and IP logs.
    """
    while True:
        data_changed = False
        for profile in profiles_config:
            try:
                s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                s.connect(profile["socket_path"])
                # Send status command.
                s.sendall(b"status\n")
                data = b""
                # Read until we see "END" in the output.
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    if b"END" in data:
                        break
                s.close()
                status_output = data.decode()
                
                # Parse status output.
                lines = status_output.splitlines()
                clients = []
                # Find section boundaries.
                start_index = None
                end_index = None
                for i, line in enumerate(lines):
                    if line.startswith("OpenVPN CLIENT LIST"):
                        start_index = i
                    if line.startswith("ROUTING TABLE"):
                        end_index = i
                        break
                
                # Parse client data if the section was found.
                if start_index is not None and end_index is not None:
                    # We assume the header line is at start_index+2.
                    # The client data lines follow until the ROUTING TABLE line.
                    for line in lines[start_index+3:end_index]:
                        if not line.strip():
                            continue
                        parts = line.split(',')
                        if len(parts) >= 5:
                            common_name = parts[0].strip()
                            real_address = parts[1].strip()
                            connected_since = parts[4].strip()
                            try:
                                conn_time = datetime.datetime.strptime(connected_since, "%Y-%m-%d %H:%M:%S")
                                runtime = str(datetime.datetime.now() - conn_time).split('.')[0]
                            except Exception as ex:
                                runtime = "N/A"
                            clients.append({
                                "common_name": common_name,
                                "real_address": real_address,
                                "connected_since": connected_since,
                                "runtime": runtime
                            })
                            # Extract the IP part (before the colon) and update the IP log.
                            ip = real_address.split(':')[0]
                            if common_name not in profile_ip_log:
                                profile_ip_log[common_name] = set()
                                data_changed = True
                            if ip not in profile_ip_log[common_name]:
                                profile_ip_log[common_name].add(ip)
                                data_changed = True
                
                # Check if client data has changed
                old_clients = profile_data.get(profile["name"], [])
                if len(old_clients) != len(clients) or any(old != new for old, new in zip(old_clients, clients)):
                    data_changed = True
                
                profile_data[profile["name"]] = clients
            except Exception as e:
                # If unable to connect or parse, clear the client list for this profile.
                if profile["name"] in profile_data and profile_data[profile["name"]]:
                    profile_data[profile["name"]] = []
                    data_changed = True
        
        # If data has changed, emit update to clients
        if data_changed:
            socketio.emit('data_update', {
                'profile_data': profile_data,
                'profile_ip_log': {k: list(v) for k, v in profile_ip_log.items()}  # Convert sets to lists for JSON
            })
        
        time.sleep(5)  # Update every 5 seconds.
=======
class ConnectionHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    profile = db.Column(db.String(100), nullable=False)
    common_name = db.Column(db.String(100), nullable=False)
    real_address = db.Column(db.String(100), nullable=False)
    location = db.Column(db.String(200))
    connected_since = db.Column(db.DateTime, nullable=False)
    disconnected_at = db.Column(db.DateTime, nullable=False)
    runtime = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
    disconnect_type = db.Column(db.String(50), default="client-side")
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)

    def to_dict(self):
        result = {
            "profile": self.profile,
            "common_name": self.common_name,
            "real_address": self.real_address,
            "location": self.location,
            "connected_since": self.connected_since.strftime("%Y-%m-%d %H:%M:%S"),
            "disconnected_at": self.disconnected_at.strftime("%Y-%m-%d %H:%M:%S"),
            "runtime": self.runtime,
            "lat": self.lat,
            "lon": self.lon,
            "disconnect_type": self.disconnect_type or "client-side"
        }
        return result

connection_history = deque(maxlen=100)

def check_and_migrate_database():
    with app.app_context():
        db.create_all()
        inspector = inspect(db.engine)
        columns = [col['name'] for col in inspector.get_columns('connection_history')]
        if 'disconnect_type' not in columns:
            logger.info("Adding disconnect_type column to connection_history table")
            try:
                db.session.execute(text('ALTER TABLE connection_history ADD COLUMN disconnect_type VARCHAR(50) DEFAULT "client-side"'))
                db.session.commit()
                logger.info("Successfully added disconnect_type column")
            except Exception as e:
                logger.error(f"Error adding disconnect_type column: {e}")
                db.session.rollback()

def load_connection_history():
    global connection_history
    try:
        history_records = ConnectionHistory.query.order_by(desc(ConnectionHistory.disconnected_at)).limit(100).all()
        connection_history = deque([record.to_dict() for record in history_records], maxlen=100)
        logger.info(f"Loaded {len(connection_history)} connection history records from database")
    except Exception as e:
        logger.error(f"Error loading connection history: {e}")
        connection_history = deque(maxlen=100)

ip_location_cache = {}
previous_clients_map = {}

def get_ip_location(ip):
    if ip in ip_location_cache:
        return ip_location_cache[ip]
    try:
        response = reader.get(ip)
        if response:
            city = response.get('city', {}).get('names', {}).get('en', 'Unknown')
            country = response.get('country', {}).get('names', {}).get('en', 'Unknown')
            region = response.get('subdivisions', [{}])[0].get('names', {}).get('en', '')
            lat = response.get('location', {}).get('latitude', None)
            lon = response.get('location', {}).get('longitude', None)
            location = {"country": country, "city": city, "region": region, "lat": lat, "lon": lon}
            ip_location_cache[ip] = location
            return location
    except Exception as e:
        logger.error(f"Error getting location for IP {ip}: {e}")
    default = {"country": "Unknown", "city": "Unknown", "region": "", "lat": None, "lon": None}
    ip_location_cache[ip] = default
    return default

def add_to_connection_history(profile_name, client_data, disconnect_type="client-side"):
    try:
        disconnected_at = datetime.datetime.now()
        connected_since = datetime.datetime.strptime(client_data["connected_since"], "%Y-%m-%d %H:%M:%S")
        location_dict = get_ip_location(client_data["real_address"])
        location_str = f"{location_dict['city']}, {location_dict['country']}"
        
        with app.app_context():
            history_record = ConnectionHistory(
                profile=profile_name,
                common_name=client_data["common_name"],
                real_address=client_data["real_address"],
                location=location_str,
                connected_since=connected_since,
                disconnected_at=disconnected_at,
                runtime=client_data["runtime"],
                disconnect_type=disconnect_type,
                lat=location_dict["lat"],
                lon=location_dict["lon"]
            )
            db.session.add(history_record)
            db.session.commit()
            
            history_entry = history_record.to_dict()
            connection_history.appendleft(history_entry)
        return True
    except Exception as e:
        logger.error(f"Error adding connection to history DB: {e}")
        with app.app_context():
            db.session.rollback()
        return False

def update_profile_status():
    global previous_clients_map
    with app.app_context():
        while True:
            data_changed = False
            current_clients_map = {}
            for profile in profiles_config:
                try:
                    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.connect(profile["socket_path"])
                    s.sendall(b"status\n")
                    data = b""
                    while True:
                        chunk = s.recv(4096)
                        if not chunk or b"END" in data:
                            break
                        data += chunk
                    s.close()
                    status_output = data.decode()
                    
                    lines = status_output.splitlines()
                    clients = []
                    start_index = None
                    end_index = None
                    for i, line in enumerate(lines):
                        if line.startswith("OpenVPN CLIENT LIST"):
                            start_index = i
                        if line.startswith("ROUTING TABLE"):
                            end_index = i
                            break
                    
                    if start_index is not None and end_index is not None:
                        for line in lines[start_index+3:end_index]:
                            if not line.strip():
                                continue
                            parts = line.split(',')
                            if len(parts) >= 5:
                                common_name = parts[0].strip()
                                real_address = parts[1].strip()
                                ip = real_address.split(':')[0]
                                connected_since = parts[4].strip()
                                try:
                                    conn_time = datetime.datetime.strptime(connected_since, "%Y-%m-%d %H:%M:%S")
                                    runtime = str(datetime.datetime.now() - conn_time).split('.')[0]
                                except Exception as ex:
                                    logger.error(f"Error parsing connection time: {ex}")
                                    runtime = "N/A"
                                location_dict = get_ip_location(ip)
                                location_str = f"{location_dict['city']}, {location_dict['country']}"
                                client_data = {
                                    "common_name": common_name,
                                    "real_address": ip,
                                    "real_address_full": real_address,
                                    "connected_since": connected_since,
                                    "runtime": runtime,
                                    "location": location_str,
                                    "lat": location_dict["lat"],
                                    "lon": location_dict["lon"]
                                }
                                clients.append(client_data)
                                client_key = (profile["name"], common_name)
                                current_clients_map[client_key] = client_data
                                if common_name not in profile_ip_log:
                                    profile_ip_log[common_name] = set()
                                    data_changed = True
                                if ip not in profile_ip_log[common_name]:
                                    profile_ip_log[common_name].add(ip)
                                    data_changed = True
                    
                    old_clients = profile_data.get(profile["name"], [])
                    if len(old_clients) != len(clients) or any(old != new for old, new in zip(old_clients, clients)):
                        data_changed = True
                    profile_data[profile["name"]] = clients
                except Exception as e:
                    logger.error(f"Error updating profile {profile['name']}: {e}")
                    if profile["name"] in profile_data and profile_data[profile["name"]]:
                        profile_data[profile["name"]] = []
                        data_changed = True
            
            for client_key, client_data in list(previous_clients_map.items()):
                if client_key not in current_clients_map:
                    profile_name, common_name = client_key
                    logger.info(f"Client disconnected: {common_name} from {profile_name} (client-side)")
                    if add_to_connection_history(profile_name, client_data, "client-side"):
                        data_changed = True
            
            previous_clients_map = current_clients_map.copy()
            
            if data_changed:
                socketio.emit('data_update', {
                    'profile_data': profile_data,
                    'profile_ip_log': {k: list(v) for k, v in profile_ip_log.items()},
                    'connection_history': list(connection_history)
                })
            time.sleep(1)
>>>>>>> Stashed changes

@app.route('/')
@auth.login_required
def index():
    ip_log_for_template = {k: list(v) for k, v in profile_ip_log.items()}
<<<<<<< Updated upstream
    return render_template('index.html', profile_data=profile_data, profile_ip_log=ip_log_for_template)
=======
    return render_template('index.html', 
                         profile_data=profile_data, 
                         profile_ip_log=ip_log_for_template,
                         connection_history=list(connection_history))
>>>>>>> Stashed changes

@app.route('/kill/<profile_name>/<client_name>', methods=["POST"])
@auth.login_required
def kill_client(profile_name, client_name):
    profile = next((p for p in profiles_config if p["name"] == profile_name), None)
    if not profile:
        flash("Profile not found", "error")
        return redirect(url_for("index"))
<<<<<<< Updated upstream
=======
    
    client_data = None
    if profile_name in profile_data:
        for client in profile_data[profile_name]:
            if client.get("common_name") == client_name:
                client_data = client
                break
    
>>>>>>> Stashed changes
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(profile["socket_path"])
        banner = s.recv(4096).decode()
        cmd = f"kill {client_name}\n"
        s.sendall(cmd.encode())
        data = b""
        while True:
            chunk = s.recv(4096)
            if not chunk or b"SUCCESS" in data or b"ERROR" in data:
                break
            data += chunk
            if len(data) > 8192:
                break
        response = data.decode().strip()
        s.close()
        
        if "SUCCESS" in response:
            flash(f"Successfully killed connection for {client_name}", "success")
<<<<<<< Updated upstream
=======
            if client_data:
                add_to_connection_history(profile_name, client_data, "admin-kill")
                client_key = (profile_name, client_name)
                if client_key in previous_clients_map:
                    del previous_clients_map[client_key]
            else:
                flash("Client data not found", "warning")
>>>>>>> Stashed changes
        else:
            flash(f"Kill command response: {response}", "info")
    except Exception as e:
        flash(f"Error sending kill command: {e}", "error")
    return redirect(url_for("index"))

def start_background_thread():
    threading.Thread(target=update_profile_status, daemon=True).start()

if __name__ == '__main__':
<<<<<<< Updated upstream
=======
    with app.app_context():
        check_and_migrate_database()
        load_connection_history()
>>>>>>> Stashed changes
    start_background_thread()
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)