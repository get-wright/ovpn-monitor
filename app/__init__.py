from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import os

from .models import db

def create_app(config_class='config.Config'):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    db.init_app(app)
    migrate = Migrate(app, db)
    
    from .routes import routes
    app.register_blueprint(routes)
    
    # Create database tables if they don't exist
    with app.app_context():
        db.create_all()
    
    return app