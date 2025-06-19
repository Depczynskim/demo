"""
Error handling utilities for POPS Analytics System.
"""
import functools
import time
from typing import Any, Callable, Type, Union, Tuple
from .logging import get_logger

logger = get_logger(__name__)

class DataExtractionError(Exception):
    """Base exception for data extraction errors."""
    pass

class ApiError(Exception):
    """Base exception for API-related errors."""
    pass

def handle_exceptions(
    error_types: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    default_value: Any = None
) -> Callable:
    """Decorator to handle exceptions and return a default value."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except error_types as e:
                logger.error(f"Error in {func.__name__}: {str(e)}")
                return default_value
        return wrapper
    return decorator

def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception
) -> Callable:
    """Decorator to retry a function on failure."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            current_delay = delay
            
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt == max_attempts:
                        logger.error(f"Final retry attempt failed for {func.__name__}: {str(e)}")
                        raise
                    
                    logger.warning(f"Attempt {attempt} failed for {func.__name__}: {str(e)}")
                    logger.info(f"Retrying in {current_delay} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            return None  # Should never reach here
        return wrapper
    return decorator 