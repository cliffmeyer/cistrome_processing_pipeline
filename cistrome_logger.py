import logging

class cistrome_logger():

    def __init__(self,logger_name,log_file):
        # Create a custom logger
        self.logger = logging.getLogger(logger_name)

        # Create handlers
        self.f_handler = logging.FileHandler(log_file)
        #self.f_handler.setLevel(logging.DEBUG)

        # Create formatters and add it to handlers
        self.f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.f_handler.setFormatter(self.f_format)

        # Add handlers to the logger
        self.logger.addHandler(self.f_handler)
        self.logger.setLevel(logging.DEBUG)


if __name__ == '__main__':

    test_logger = cistrome_logger(__name__,'tmp.log')
    test_logger.logger.warning('This is a warning')
    test_logger.logger.error('This is an error')                                                           
