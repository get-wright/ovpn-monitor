from flask import Flask, render_template, redirect, url_for, request, flash
import socket
import datetime
import threading
import time
from flask_socketio import SocketIO
import requests
import json
from collections import deque

app = Flask(__name__)
app.secret_key = 'secret!'
socketio = SocketIO(app)

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

# Connection history (limited to the most recent 100 entries)
# Contains disconnected clients with their information
connection_history = deque(maxlen=100)  # [{common_name, real_address, location, connected_since, disconnected_at, runtime}]

# Cache for IP geolocation data
ip_location_cache = {}  # {ip: {country: ..., city: ...}}

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
        print(f"Error getting location for IP {ip}: {e}")
    
    # Return default if API call fails
    default = {"country": "Unknown", "city": "Unknown", "region": ""}
    ip_location_cache[ip] = default
    return default

def update_profile_status():
    """
    Background thread function that periodically connects to each OpenVPN management interface,
    sends the "status" command, parses the output, and updates the client list and IP logs.
    """
    # Keep track of active clients to detect disconnections
    previous_active_clients = set()  # {(profile_name, common_name)}
    
    while True:
        data_changed = False
        current_active_clients = set()  # Track currently active clients
        
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
                            
                            # Add to set of active clients
                            client_key = (profile["name"], common_name)
                            current_active_clients.add(client_key)
                            
                            try:
                                conn_time = datetime.datetime.strptime(connected_since, "%Y-%m-%d %H:%M:%S")
                                runtime = str(datetime.datetime.now() - conn_time).split('.')[0]
                            except Exception as ex:
                                runtime = "N/A"
                                
                            # Extract the IP part (before the colon)
                            ip = real_address.split(':')[0]
                            
                            # Get location information for the IP
                            location = get_ip_location(ip)
                            location_str = f"{location['city']}, {location['country']}"
                            
                            clients.append({
                                "common_name": common_name,
                                "real_address": real_address,
                                "connected_since": connected_since,
                                "runtime": runtime,
                                "location": location_str
                            })
                            
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
                # If unable to connect or parse, clear the client list for this profile.
                if profile["name"] in profile_data and profile_data[profile["name"]]:
                    profile_data[profile["name"]] = []
                    data_changed = True
        
        # Check for disconnected clients
        disconnected = previous_active_clients - current_active_clients
        if disconnected:
            data_changed = True
            for profile_name, common_name in disconnected:
                # Find the client in the previous data
                for client in profile_data.get(profile_name, []):
                    if client.get("common_name") == common_name:
                        # Add to connection history
                        disconnected_at = datetime.datetime.now()
                        ip = client["real_address"].split(':')[0]
                        location = get_ip_location(ip)
                        location_str = f"{location['city']}, {location['country']}"
                        
                        connection_history.appendleft({
                            "common_name": common_name,
                            "real_address": client["real_address"],
                            "connected_since": client["connected_since"],
                            "disconnected_at": disconnected_at.strftime("%Y-%m-%d %H:%M:%S"),
                            "runtime": client["runtime"],
                            "location": location_str,
                            "profile": profile_name
                        })
                        break
        
        # Update previous active clients for next iteration
        previous_active_clients = current_active_clients
        
        # If data has changed, emit update to clients
        if data_changed:
            socketio.emit('data_update', {
                'profile_data': profile_data,
                'profile_ip_log': {k: list(v) for k, v in profile_ip_log.items()},  # Convert sets to lists for JSON
                'connection_history': list(connection_history)  # Convert deque to list for JSON
            })
        
        time.sleep(5)  # Update every 5 seconds.

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
        else:
            flash(f"Kill command response: {response}", "info")
    except Exception as e:
        flash(f"Error sending kill command: {e}", "error")
    return redirect(url_for("index"))

# Start the background thread.
def start_background_thread():
    threading.Thread(target=update_profile_status, daemon=True).start()

if __name__ == '__main__':
    start_background_thread()
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)