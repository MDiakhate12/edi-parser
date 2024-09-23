import logging


class CustomAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['simu_id'], msg), kwargs 


class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.log_records = []

    def emit(self, record):
        # Append the formatted log message to the list
        self.log_records.append(self.format(record))


class CustomLogger:

    def configure_logger(self, simulation_id: str):
        """
        Configure a logger with a custom adapter to prepend the simulation ID to log messages.
        
        This function initializes a logger with the given name and sets its level to INFO. 
        The logger is then wrapped in a CustomAdapter to prepend each log message with the 
        simulation ID provided as an argument. This helps in differentiating log entries 
        originating from different simulation runs.
        
        Parameters:
        - simulation_id (str): The ID of the simulation, which will be prepended to log messages.
        
        Returns:
        - CustomAdapter: A logger adapter that prepends the simulation ID to log messages.
        
        Example Usage:
        >>> logger = configure_logger("12345")
        >>> logger.info("This is a log message.")
        [12345] This is a log message.
        """
        logger_config = logging.getLogger(__name__)
        logger_config.setLevel(logging.INFO)

        # Create and add the list handler
        list_handler = ListHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        list_handler.setFormatter(formatter)
        logger_config.addHandler(list_handler)

        return CustomAdapter(logger_config, {'simu_id': simulation_id}), list_handler
    

    def __init__(self, simulation_id) -> None:
        self.logger, self.list_handler = self.configure_logger(simulation_id)