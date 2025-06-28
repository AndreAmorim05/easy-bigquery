# easy_bigquery/logger/manager.py
"""
Application logging setup module using Loguru.

This module configures and exposes a centralized 'logger' object,
ready to be imported and used throughout the application. It handles
formatting, levels, and log destinations (sinks) for both console
and file outputs in a simple and efficient manner.
"""
import sys

from loguru import logger

# Remove the default handler to gain full control over the sinks.
logger.remove()

# Define a shared format for consistent logging.
log_format = (
    '<green>{time:YYYY-MM-DD HH:mm:ss}</green> | '
    '<level>{level: <8}</level> | '
    '<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - '
    '<level>{message}</level>'
)

# Add a sink for console output with rich, colored formatting.
logger.add(
    sys.stderr,
    level='INFO',
    format=log_format,
    colorize=True
)

# Add a sink for file output with built-in rotation and retention.
# logger.add(
#     'workflow_fetch.log',
#     level='INFO',
#     format='{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}',
#     rotation='10 MB',          # Rotates the log file when it reaches 10 MB.
#     retention='7 days',        # Keeps log files for a maximum of 7 days.
#     enqueue=True,              # Makes logging thread-safe and process-safe.
#     backtrace=True,            # Shows the full stack trace on exceptions.
#     diagnose=True,             # Adds exception variable values for debugging.
#     mode='a'
# )
