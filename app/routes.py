from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request
from datetime import datetime
from .openvpn import get_active_profiles, kill_profile
from .models import db, IPLog, OpenVPNProfile

routes = Blueprint('routes', __name__)

@routes.route('/')
def index():
    profiles = get_active_profiles()
    return render_template('index.html', profiles=profiles)

@routes.route('/kill_profile/<int:profile_id>', methods=['POST'])
def kill_profile_route(profile_id):
    success = kill_profile(profile_id)
    
    # Handle AJAX requests
    if request.headers.get('Content-Type') == 'application/json':
        if success:
            return jsonify({"success": True, "message": "Profile killed successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to kill profile"})
    
    # Handle form submissions
    if success:
        flash('Profile has been killed successfully.')
    else:
        flash('Failed to kill profile.')
    return redirect(url_for('routes.index'))

@routes.route('/view_ip_log/<int:profile_id>')
def view_ip_log(profile_id):
    profile = OpenVPNProfile.query.get_or_404(profile_id)
    ip_logs = IPLog.query.filter_by(profile_id=profile_id).order_by(IPLog.timestamp.desc()).all()
    return render_template('profile.html', profile_name=profile.name, ip_logs=ip_logs)

@routes.context_processor
def utility_processor():
    def get_current_year():
        return datetime.now().year
    return dict(current_year=get_current_year)