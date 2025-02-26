import subprocess
import datetime
import re
from .models import db, OpenVPNProfile, IPLog
from .utils import format_runtime

def send_command(command):
    """Send command to OpenVPN management interface via Unix socket."""
    try:
        # Use subprocess to execute the command with sudo
        cmd = ['sudo', 'socat', '-', 'UNIX-CONNECT:/run/openvpn/pt.sock']
        process = subprocess.Popen(cmd, 
                                  stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  universal_newlines=True)
        
        # Send the command
        stdout, stderr = process.communicate(input=f"{command}\n")
        
        if process.returncode != 0:
            print(f"Error executing command: {stderr}")
            return None
            
        return stdout
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def parse_status(response):
    """Parse OpenVPN status output to extract client information."""
    if not response:
        return []
    
    profiles = []
    lines = response.splitlines()
    
    # Find the CLIENT LIST section
    in_client_list = False
    header_skipped = False
    
    for line in lines:
        line = line.strip()
        
        if line == "OpenVPN CLIENT LIST":
            in_client_list = True
            continue
            
        if in_client_list and not header_skipped and line.startswith("Updated"):
            header_skipped = True
            continue
            
        if in_client_list and header_skipped and line.startswith("Common Name"):
            continue
            
        if in_client_list and header_skipped:
            if line.startswith("ROUTING TABLE") or not line:
                break
                
            # Parse client entries
            parts = line.split(',')
            if len(parts) >= 5:  # Ensure we have enough parts
                common_name = parts[0].strip()
                real_address = parts[1].strip()
                ip = real_address.split(':')[0] if ':' in real_address else real_address
                connected_since = parts[4].strip()
                
                profile = {
                    'name': common_name,
                    'ip': ip,
                    'connected_since': connected_since
                }
                profiles.append(profile)
    
    return profiles

def update_ip_log(profiles):
    """Update the IP log database with current profile information."""
    current_time = datetime.datetime.now()
    
    for profile_data in profiles:
        # Skip UNDEF profiles
        if profile_data['name'] == 'UNDEF':
            continue
            
        # Get or create profile
        profile = OpenVPNProfile.query.filter_by(name=profile_data['name']).first()
        if not profile:
            profile = OpenVPNProfile(
                name=profile_data['name'],
                ip_address=profile_data['ip']
            )
            db.session.add(profile)
            db.session.commit()
        
        # Update profile's current IP
        profile.ip_address = profile_data['ip']
        db.session.commit()
        
        # Check if this IP is new for this profile
        ip_log = IPLog.query.filter_by(
            profile_id=profile.id, 
            ip_address=profile_data['ip']
        ).first()
        
        if not ip_log:
            # Log new IP
            new_log = IPLog(
                profile_id=profile.id,
                ip_address=profile_data['ip'],
                timestamp=current_time
            )
            db.session.add(new_log)
            db.session.commit()

def get_active_profiles():
    """Get list of active profiles with runtime calculation."""
    response = send_command('status')
    if not response:
        return []
    
    profiles = parse_status(response)
    update_ip_log(profiles)
    
    # Calculate runtime for each profile
    current_time = datetime.datetime.now()
    result = []
    
    for profile_data in profiles:
        # Skip UNDEF profiles
        if profile_data['name'] == 'UNDEF':
            continue
            
        profile = OpenVPNProfile.query.filter_by(name=profile_data['name']).first()
        if not profile:
            continue
            
        try:
            connected_time = datetime.datetime.strptime(
                profile_data['connected_since'], 
                '%Y-%m-%d %H:%M:%S'
            )
            runtime_seconds = int((current_time - connected_time).total_seconds())
            runtime_formatted = format_runtime(runtime_seconds)
            
            result.append({
                'id': profile.id,
                'name': profile.name,
                'runtime': runtime_formatted,
                'ip': profile.ip_address
            })
        except Exception as e:
            print(f"Error calculating runtime: {e}")
    
    return result

def kill_profile(profile_id):
    """Kill an OpenVPN profile by its ID."""
    profile = OpenVPNProfile.query.get(profile_id)
    if not profile:
        return False
    
    response = send_command(f'kill {profile.name}')
    
    # Check if the kill was successful
    if response and "SUCCESS" in response:
        return True
    return False