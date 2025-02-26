from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class OpenVPNProfile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    ip_address = db.Column(db.String(45), nullable=False)  # Support for IPv6 addresses

    def __repr__(self):
        return f'<OpenVPNProfile {self.name}>'

class IPLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    profile_id = db.Column(db.Integer, db.ForeignKey('open_vpn_profile.id'), nullable=False)
    ip_address = db.Column(db.String(45), nullable=False)  # Support for IPv6 addresses
    timestamp = db.Column(db.DateTime, default=datetime.now)
    
    profile = db.relationship('OpenVPNProfile', backref=db.backref('ip_logs', lazy=True))

    def __repr__(self):
        return f'<IPLog {self.ip_address}>'