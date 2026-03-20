"""
Sentry error monitoring — optional.

Set the SENTRY_DSN environment variable (GitHub Actions secret) to enable.
Free Sentry account available via GitHub Education Pack.

Usage anywhere in the codebase:
    from .sentry import capture
    capture(exception)
    # or
    capture("descriptive message about what failed")
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)
_initialized = False


def init():
    global _initialized
    if _initialized:
        return
    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        logger.debug("SENTRY_DSN not set — error monitoring disabled")
        return
    try:
        import sentry_sdk
        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=0.1,
            environment=os.environ.get("ENVIRONMENT", "production"),
            release=os.environ.get("GITHUB_SHA", "unknown"),
        )
        _initialized = True
        logger.info("Sentry error monitoring enabled")
    except ImportError:
        logger.warning("sentry-sdk not installed — pip install sentry-sdk to enable monitoring")


def capture(exc_or_message: Exception | str) -> None:
    """Capture an exception or message to Sentry. Safe to call even if Sentry is not configured."""
    dsn = os.environ.get("SENTRY_DSN", "").strip()
    if not dsn:
        return
    try:
        import sentry_sdk
        if isinstance(exc_or_message, Exception):
            sentry_sdk.capture_exception(exc_or_message)
        else:
            sentry_sdk.capture_message(str(exc_or_message))
    except Exception:
        pass  # Never let monitoring break the scraper
