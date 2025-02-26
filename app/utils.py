from datetime import datetime

def format_runtime(seconds):
    """Format runtime in seconds to a human-readable format."""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"

def get_current_timestamp():
    """Get the current timestamp as a string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")