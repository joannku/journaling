import logging

class Logger:

    def __init__(self, log_filename='data.log', log_level=logging.INFO, log_format='%(asctime)s - %(levelname)s - %(message)s'):
        # Set up logging configuration during initialization
        self.log_filename = log_filename
        self.log_level = log_level
        self.log_format = log_format
        
        logging.basicConfig(
            filename=self.log_filename,
            level=self.log_level,
            format=self.log_format,
            force=True  # Assuming you're using Python 3.8+ for this argument
        )

    def log_message(self, message):
        # Just log the message, no need to re-configure logging
        print(message)
        logging.info(message)