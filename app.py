from flask import Flask, render_template, redirect, url_for, request, flash
import socket
import datetime
import threading
import time
from flask_socketio import SocketIO
import requests
import json
from collections import deque
import logging
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import desc, inspect, text
import os
import pytz

app = Flask(__name__)
app.secret_key = os.urandom(32)

# Database configuration
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'ovpn_monitor.db')
os.makedirs(os.path.dirname(db_path), exist_ok=True)  # Create the data directory if it doesn't exist
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

socketio = SocketIO(app)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('ovpn-monitor')

# Configuration: list of OpenVPN management interface profiles.
# Each profile must include a unique name and its corresponding UNIX socket path.
profiles_config = [
    {"name": "profile1", "socket_path": "/run/openvpn/pt.sock"},
    # You can add more profiles here, for example:
    # {"name": "profile2", "socket_path": "/run/openvpn/profile2.sock"},
]

# In-memory dictionaries for client data and IP logging.
# profile_data maps profile names to lists of client dictionaries.
# profile_ip_log maps client common names to a set of IP addresses seen.
profile_data = {}     # { profile_name: [ { "common_name": ..., "runtime": ..., "real_address": ..., "connected_since": ... }, ... ] }
profile_ip_log = {}   # { common_name: set([ip1, ip2, ...]) }

@app.template_filter('to_utc7')
def to_utc7_filter(dt_str):
    dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    dt_utc = pytz.UTC.localize(dt)
    dt_utc7 = dt_utc.astimezone(pytz.timezone('Asia/Ho_Chi_Minh'))
    return dt_utc7.strftime("%Y-%m-%d %H:%M:%S")

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
    disconnect_type = db.Column(db.String(50), default="client-side")  # Added to track disconnect type

    def to_dict(self):
        result = {
            "profile": self.profile,
            "common_name": self.common_name,
            "real_address": self.real_address,
            "location": self.location,
            "connected_since": self.connected_since.strftime("%Y-%m-%d %H:%M:%S"),
            "disconnected_at": self.disconnected_at.strftime("%Y-%m-%d %H:%M:%S"),
            "runtime": self.runtime
        }
        
        # Add disconnect_type if it exists (to handle old records in DB)
        if hasattr(self, 'disconnect_type') and self.disconnect_type is not None:
            result["disconnect_type"] = self.disconnect_type
        else:
            result["disconnect_type"] = "client-side"  # default for old records
            
        return result

# Connection history (limited to the most recent 100 entries)
# Contains disconnected clients with their information
connection_history = deque(maxlen=100)  # [{common_name, real_address, location, connected_since, disconnected_at, runtime, profile}]

def check_and_migrate_database():
    """
    Check if the database schema matches our models and perform migrations if needed
    """
    with app.app_context():
        # Create all tables if they don't exist
        db.create_all()
        
        # Check if the disconnect_type column exists in ConnectionHistory table
        inspector = inspect(db.engine)
        columns = [col['name'] for col in inspector.get_columns('connection_history')]
        
        if 'disconnect_type' not in columns:
            logger.info("Adding disconnect_type column to connection_history table")
            # Use raw SQL to add the column since SQLAlchemy doesn't handle schema migrations
            try:
                db.session.execute(text('ALTER TABLE connection_history ADD COLUMN disconnect_type VARCHAR(50) DEFAULT "client-side"'))
                db.session.commit()
                logger.info("Successfully added disconnect_type column")
            except Exception as e:
                logger.error(f"Error adding disconnect_type column: {e}")
                db.session.rollback()
                # If we can't alter the table, we'll have to live with the model mismatch
                # The to_dict method will handle this gracefully

def load_connection_history():
    """
    Load recent connection history from the database into the in-memory cache
    """
    global connection_history
    try:
        history_records = ConnectionHistory.query.order_by(
            desc(ConnectionHistory.disconnected_at)
        ).limit(100).all()
        
        connection_history = deque(
            [record.to_dict() for record in history_records],
            maxlen=100
        )
        logger.info(f"Loaded {len(connection_history)} connection history records from database")
    except Exception as e:
        logger.error(f"Error loading connection history: {e}")
        connection_history = deque(maxlen=100)  # Initialize empty if there's an error

# Cache for IP geolocation data
ip_location_cache = {}  # {ip: {country: ..., city: ...}}

# Store a copy of previous clients for tracking disconnections 
previous_clients_map = {}  # {(profile_name, common_name): client_data}

def get_ip_location(ip):
    """
    Get location information for an IP address using a free geolocation API.
    Caches results to avoid repeated API calls for the same IP.
    """
    # Return cached result if available
    if ip in ip_location_cache:
        return ip_location_cache[ip]
    
    try:
        # Use ip-api.com which is free and doesn't require API key
        response = requests.get(f"http://ip-api.com/json/{ip}", timeout=2)
        if response.status_code == 200:
            data = response.json()
            if data.get("status") == "success":
                location = {
                    "country": data.get("country", "Unknown"),
                    "city": data.get("city", "Unknown"),
                    "region": data.get("regionName", "")
                }
                # Cache the result
                ip_location_cache[ip] = location
                return location
    except Exception as e:
        logger.error(f"Error getting location for IP {ip}: {e}")
    
    # Return default if API call fails
    default = {"country": "Unknown", "city": "Unknown", "region": ""}
    ip_location_cache[ip] = default
    return default

def add_to_connection_history(profile_name, client_data, disconnect_type="client-side"):
    """
    Helper function to add a client to the connection history database and in-memory cache
    Returns True if successful, False otherwise
    """
    try:
        disconnected_at = datetime.datetime.now()
        # Parse the connected_since string into a datetime object
        connected_since = datetime.datetime.strptime(
            client_data["connected_since"], 
            "%Y-%m-%d %H:%M:%S"
        )
        
        # Create a new session for this specific operation
        with app.app_context():
            # Create the record within the context
            history_record = ConnectionHistory(
                profile=profile_name,
                common_name=client_data["common_name"],
                real_address=client_data["real_address"],
                location=client_data["location"],
                connected_since=connected_since,
                disconnected_at=disconnected_at,
                runtime=client_data["runtime"],
                disconnect_type=disconnect_type
            )
            
            # Add to session and commit within the context
            db.session.add(history_record)
            db.session.commit()
            
            # Create the dict representation while still in context
            history_entry = {
                "profile": history_record.profile,
                "common_name": history_record.common_name,
                "real_address": history_record.real_address.split(':')[0] if ':' in history_record.real_address else history_record.real_address,
                "location": history_record.location,
                "connected_since": connected_since.strftime("%Y-%m-%d %H:%M:%S"),
                "disconnected_at": disconnected_at.strftime("%Y-%m-%d %H:%M:%S"),
                "runtime": history_record.runtime,
                "disconnect_type": history_record.disconnect_type
            }
        
        # Now outside the context, we can safely use the dict representation
        connection_history.appendleft(history_entry)
        
        logger.info(f"Added to history DB: {client_data['common_name']} ({disconnect_type})")
        return True
    except Exception as e:
        logger.error(f"Error adding connection to history DB: {e}")
        try:
            # Ensure we rollback if there was an error
            with app.app_context():
                db.session.rollback()
        except:
            pass
        return False

def update_profile_status():
    """
    Background thread function that periodically connects to each OpenVPN management interface,
    sends the "status" command, parses the output, and updates the client list and IP logs.
    """
    global previous_clients_map
    
    # Create an application context for this thread
    with app.app_context():
        while True:
            data_changed = False
            current_clients_map = {}  # Track currently active clients with their data
            
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
                                ip = real_address.split(':')[0]
                                connected_since = parts[4].strip()
                                
                                try:
                                    conn_time = datetime.datetime.strptime(connected_since, "%Y-%m-%d %H:%M:%S")
                                    runtime = str(datetime.datetime.now() - conn_time).split('.')[0]
                                except Exception as ex:
                                    logger.error(f"Error parsing connection time: {ex}")
                                    runtime = "N/A"
                                    
                                # Extract the IP part (before the colon)
                                ip = real_address.split(':')[0]
                                
                                # Get location information for the IP
                                location = get_ip_location(ip)
                                location_str = f"{location['city']}, {location['country']}"
                                
                                client_data = {
                                    "common_name": common_name,
                                    "real_address": ip,  # Store only IP address, not port
                                    "real_address_full": real_address,  # Keep full address in a separate field
                                    "connected_since": connected_since,
                                    "runtime": runtime,
                                    "location": location_str
                                }
                                
                                clients.append(client_data)
                                
                                # Store in current clients map
                                client_key = (profile["name"], common_name)
                                current_clients_map[client_key] = client_data
                                
                                # Update the IP log
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
                    logger.error(f"Error updating profile {profile['name']}: {e}")
                    # If unable to connect or parse, clear the client list for this profile.
                    if profile["name"] in profile_data and profile_data[profile["name"]]:
                        profile_data[profile["name"]] = []
                        data_changed = True
            
            # Check for disconnected clients
            for client_key, client_data in list(previous_clients_map.items()):
                if client_key not in current_clients_map:
                    profile_name, common_name = client_key
                    logger.info(f"Client disconnected: {common_name} from {profile_name} (client-side)")
                    
                    # Add to connection history
                    if add_to_connection_history(profile_name, client_data, "client-side"):
                        data_changed = True
            
            # Update previous clients map for next iteration
            previous_clients_map = current_clients_map.copy()
            
            # If data has changed, emit update to clients
            if data_changed:
                socketio.emit('data_update', {
                    'profile_data': profile_data,
                    'profile_ip_log': {k: list(v) for k, v in profile_ip_log.items()},  # Convert sets to lists for JSON
                    'connection_history': list(connection_history)  # Convert deque to list for JSON
                })
            
            time.sleep(1)  # Update every 1 seconds.

@app.route('/')
def index():
    # Convert sets to lists for initial template render
    ip_log_for_template = {k: list(v) for k, v in profile_ip_log.items()}
    return render_template('index.html', 
                           profile_data=profile_data, 
                           profile_ip_log=ip_log_for_template,
                           connection_history=list(connection_history))

@app.route('/kill/<profile_name>/<client_name>', methods=["POST"])
def kill_client(profile_name, client_name):
    """
    Connect to the management interface for the given profile and send the kill command for the specified client.
    """
    # Find the profile configuration.
    profile = next((p for p in profiles_config if p["name"] == profile_name), None)
    if not profile:
        flash("Profile not found", "error")
        return redirect(url_for("index"))
    
    # Find client data before killing
    client_data = None
    if profile_name in profile_data:
        for client in profile_data[profile_name]:
            if client.get("common_name") == client_name:
                client_data = client
                break
    
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(profile["socket_path"])
        
        # First, read the welcome banner
        banner = s.recv(4096).decode()
        
        # Send kill command
        cmd = f"kill {client_name}\n"
        s.sendall(cmd.encode())
        
        # Read the complete response
        data = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
            # Kill command typically returns a SUCCESS or ERROR message
            if b"SUCCESS" in data or b"ERROR" in data:
                break
            # Add timeout safety
            if len(data) > 8192:  # Limit response size
                break
                
        response = data.decode().strip()
        s.close()
        
        if "SUCCESS" in response:
            flash(f"Successfully killed connection for {client_name}", "success")
            
            # Add to connection history if we found the client data
            if client_data:
                # Use the helper function with admin-kill as the disconnect type
                add_to_connection_history(profile_name, client_data, "admin-kill")
                
                # Also remove this client from the previous_clients_map to prevent duplicate entries
                client_key = (profile_name, client_name)
                if client_key in previous_clients_map:
                    del previous_clients_map[client_key]
            else:
                flash("Client data not found", "warning")
        else:
            flash(f"Kill command response: {response}", "info")
    except Exception as e:
        flash(f"Error sending kill command: {e}", "error")
    return redirect(url_for("index"))

# Start the background thread.
def start_background_thread():
    threading.Thread(target=update_profile_status, daemon=True).start()

if __name__ == '__main__':
    with app.app_context():
        # Check for database schema changes and migrate as needed
        check_and_migrate_database()
        # Load history after making sure the schema is correct
        load_connection_history()
    start_background_thread()
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)