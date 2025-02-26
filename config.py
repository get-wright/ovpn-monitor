import os
from pathlib import Path

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or 'sqlite:///openvpn_monitor.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    OPENVPN_SOCKET_PATH = '/run/openvpn/pt.sock'