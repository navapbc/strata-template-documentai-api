from documentai_api.logging.config import LoggingContext
from documentai_api.logging.logger import get_logger as get_logger


def init(program_name: str) -> LoggingContext:
    return LoggingContext(program_name)
