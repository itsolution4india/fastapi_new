import logging
import os
import random


log_directory = "logs"
log_file = "app.log"

# Ensure log directory exists
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Configure logging to log to both console and a file
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more verbose logging
    format="%(asctime)s [%(levelname)s] %(message)s",  # Include timestamp
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),  # Log to console (stdout)
        logging.FileHandler(os.path.join(log_directory, log_file))  # Log to file
    ]
)

logger = logging.getLogger(__name__)

def generate_unique_id() -> str:
    """Generate a 16-digit unique number."""
    return ''.join([str(random.randint(0, 9)) for _ in range(16)])