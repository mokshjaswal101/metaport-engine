"""
Celery Application Configuration

This module configures Celery for background task processing.
Used primarily for report generation but extensible for other background tasks.

Usage:
    Start worker: celery -A celery_app worker --loglevel=info --pool=solo (Windows)
    Start worker: celery -A celery_app worker --loglevel=info --concurrency=4 (Linux/Mac)
"""

import os
from celery import Celery
from kombu import Queue
from dotenv import load_dotenv

load_dotenv()

# Redis configuration from environment variables
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
REDIS_DB = os.environ.get("REDIS_DB", "0")

# Build Redis URL
if REDIS_PASSWORD:
    REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
else:
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# Initialize Celery app
celery_app = Celery(
    "metaport_worker",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=[
        "modules.reports.celery_tasks",  # Report generation tasks
    ],
)

# Celery Configuration
celery_app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Task execution settings
    task_acks_late=True,  # Acknowledge task after completion (prevents task loss on worker crash)
    task_reject_on_worker_lost=True,  # Reject task if worker dies
    task_time_limit=1800,  # 30 minutes hard timeout
    task_soft_time_limit=1500,  # 25 minutes soft timeout (raises exception)
    # Worker settings
    worker_prefetch_multiplier=1,  # Fetch one task at a time (better for long-running tasks)
    worker_max_tasks_per_child=100,  # Restart worker after 100 tasks (prevents memory leaks)
    # Result backend settings
    result_expires=86400,  # Results expire after 24 hours
    result_extended=True,  # Store additional task metadata
    # Queue configuration
    task_queues=(
        Queue("reports", routing_key="reports.#"),
        # Future: Add priority queue when needed
        # Queue("reports_priority", routing_key="reports.priority.#"),
    ),
    task_default_queue="reports",
    task_default_routing_key="reports.default",
    # Retry settings
    task_default_retry_delay=60,  # 1 minute delay between retries
    task_max_retries=3,
    # Beat scheduler (for future scheduled tasks)
    # beat_schedule={},
)

# Task routing (for future use with multiple queues)
celery_app.conf.task_routes = {
    "modules.reports.celery_tasks.*": {"queue": "reports"},
    # Future: Route priority tasks
    # "modules.reports.celery_tasks.generate_priority_report": {"queue": "reports_priority"},
}


def get_celery_app() -> Celery:
    """Get the configured Celery application instance."""
    return celery_app
