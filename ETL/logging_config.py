import logging
import os

# Define a log directory for logs
log_dir = 'Logs'
os.makedirs(log_dir,exist_ok=True) # Creates logs directory if not exists.

# Configure Logging
log_file = os.path.join(log_dir,"etl_pipeline.log")

# Configuration
logging.basicConfig(
    filename = log_file, # Save to logs to a file
    level = logging.INFO, # Set logging level to info - information.
    format = "%(asctime)s - %(levelname)s - %(message)s", # Format on how does log saves.
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Create a logger instance
logger = logging.getLogger(__name__)
