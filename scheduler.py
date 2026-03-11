"""Scheduler for periodic scraping.

Load .env before any local imports so env vars are available at module load time.

Runs as a background process alongside the web server.
Uses APScheduler for cron-like scheduling.

Schedule:
  - Daily at 06:00: full scrape
  - Wednesdays every 2h from 08:00-20:00: catch new programme (Kinowoche starts Mi)
  - Every 6h on other days: keep data fresh
"""
from dotenv import load_dotenv
load_dotenv()

import logging
import os
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from orchestrator import run_scrape
from telegram_bot import notify_new_film, notify_all_pending

logger = logging.getLogger(__name__)


def scrape_and_notify():
    """Run scraper and send Telegram notifications for new films."""
    logger.info(f"Starting scheduled scrape at {datetime.now().isoformat()}")
    try:
        result = run_scrape(notify_callback=notify_new_film)
        if result:
            logger.info(f"Scrape done: {result['total_films']} films, {result['new_films']} new")
        # Also catch any missed notifications
        notify_all_pending()
    except Exception as e:
        logger.error(f"Scheduled scrape failed: {e}", exc_info=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    scheduler = BlockingScheduler(timezone="Europe/Berlin")

    # Daily at 06:00
    scheduler.add_job(
        scrape_and_notify,
        CronTrigger(hour=6, minute=0),
        id="daily_scrape",
        name="Daily morning scrape",
    )

    # Wednesdays: every 2 hours from 08:00-20:00
    # (new Kinowoche typically starts Wednesday)
    scheduler.add_job(
        scrape_and_notify,
        CronTrigger(day_of_week="wed", hour="8,10,12,14,16,18,20"),
        id="wednesday_scrape",
        name="Wednesday frequent scrape",
    )

    # Other days: every 6 hours
    scheduler.add_job(
        scrape_and_notify,
        CronTrigger(day_of_week="mon,tue,thu,fri,sat,sun", hour="0,6,12,18"),
        id="regular_scrape",
        name="Regular 6h scrape",
    )

    # Run once at startup
    logger.info("Running initial scrape...")
    scrape_and_notify()

    logger.info("Scheduler started. Jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  {job.name}: {job.trigger}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler shutting down...")


if __name__ == "__main__":
    main()
