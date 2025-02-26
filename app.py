from flask import Flask, render_template, redirect, url_for, request, flash
import socket
import datetime
import threading
import time

app = Flask(__name__)
app.secret_key = 'secret!'

# Configuration: list of OpenVPN management interface profiles.
# Each profile must include a unique name and its corresponding UNIX socket path.
profiles_config = [
    {"name": "OVPN_tracker", "socket_path": "/run/openvpn/pt.sock"},
    # You can add more profiles here, for example:
    # {"name": "profile2", "socket_path": "/run/openvpn/profile2.sock"},
]

# In-memory dictionaries for client data and IP logging.
# profile_data maps profile names to lists of client dictionaries.
# profile_ip_log maps client common names to a set of IP addresses seen.
profile_data = {}     # { profile_name: [ { "common_name": ..., "runtime": ..., "real_address": ..., "connected_since": ... }, ... ] }
profile_ip_log = {}   # { common_name: set([ip1, ip2, ...]) }

def update_profile_status():
    """
    Background thread function that periodically connects to each OpenVPN management interface,
    sends the "status" command, parses the output, and updates the client list and IP logs.
    """
    while True:
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
                # We assume the output format is similar to:
                #   OpenVPN CLIENT LIST
                #   Updated,YYYY-MM-DD HH:MM:SS
                #   Common Name,Real Address,Bytes Received,Bytes Sent,Connected Since
                #   <client line 1>
                #   <client line 2>
                #   ...
                #   ROUTING TABLE
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
                            profile_ip_log[common_name].add(ip)
                profile_data[profile["name"]] = clients
            except Exception as e:
                # If unable to connect or parse, clear the client list for this profile.
                profile_data[profile["name"]] = []
        time.sleep(5)  # Update every 5 seconds.

# Start the background thread.
threading.Thread(target=update_profile_status, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html', profile_data=profile_data, profile_ip_log=profile_ip_log)

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
