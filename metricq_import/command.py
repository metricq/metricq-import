from datetime import timedelta
import logging

import click
import click_log
import click_completion
import pytimeparse

from metricq.logging import get_logger

from .importer import DataheapToHTAImporter

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel('INFO')
# Use this if we ever use threads
# logger.handlers[0].formatter = logging.Formatter(fmt='%(asctime)s %(threadName)-16s %(levelname)-8s %(message)s')
logger.handlers[0].formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s')

click_completion.init()


def parse_interval(ctx, param, value):
    if not value or value.lower() in ('no', 'false', 'not'):
        return None
    return timedelta(seconds=pytimeparse.parse(value))


def command(name=''):
    def decorator(func):
        @click.option('--metricq-token', show_default=True)
        @click.option('--metricq-url', default='amqp://localhost/', show_default=True)
        @click.option('--couchdb-url', default='http://127.0.0.1:5984', show_default=True)
        @click.option('--couchdb-user', default='admin', show_default=True)
        @click.option('--couchdb-password', default='admin', prompt=True, show_default=True)
        @click.option('--import-workers', default=3, type=int, show_default=True)
        @click.option('--import-host', default='127.0.0.1', show_default=True)
        @click.option('--import-port', default=3306, type=int, show_default=True)
        @click.option('--import-user', default='admin', show_default=True)
        @click.option('--import-password', default='admin', prompt=True, show_default=True)
        @click.option('--import-database', default='db', show_default=True)
        @click.option('--dry-run', is_flag=True, default=False, show_default=True)
        @click.option('--check-values', is_flag=True, default=False, show_default=True)
        @click.option('--check-interval/--no-check-interval', is_flag=True, default=True, show_default=True)
        @click.option('--check-max-age', default='8h', callback=parse_interval,
                      help='check if the import db has recent values within a specified time range (e.g. "8h", "no")')
        @click.option('--assume-yes', '-y', is_flag=True, default=False, help='Automatic yes to prompts')
        @click.option('--quiet', '-q', is_flag=True, default=False, help='Suppress stdout from importer')
        @click.option('--ignore-out-of-range-timestamps', is_flag=True, default=False,
                      help='Ignore timestamps that are totally far in the future from broken sources')
        @click.option('--resume', is_flag=True, default=False, show_default=True)
        @click_log.simple_verbosity_option(logger)
        def wrapper(metricq_token, metricq_url,
                    couchdb_url, couchdb_user, couchdb_password,
                    import_workers,
                    import_host, import_port, import_user, import_password, import_database,
                    dry_run, check_values, check_interval, check_max_age, assume_yes, quiet,
                    ignore_out_of_range_timestamps, resume,
                    **kwargs):
            importer = DataheapToHTAImporter(
                metricq_token=metricq_token, metricq_url=metricq_url,
                couchdb_url=couchdb_url, couchdb_user=couchdb_user, couchdb_password=couchdb_password,
                import_workers=import_workers,
                import_host=import_host, import_port=import_port,
                import_user=import_user, import_password=import_password, import_database=import_database,
                dry_run=dry_run, check_values=check_values, check_interval=check_interval, check_max_age=check_max_age,
                quiet=quiet,
                assume_yes=assume_yes,
                ignore_out_of_range_timestamps=ignore_out_of_range_timestamps,
                resume=resume,
            )
            return func(importer, **kwargs)

        try:
            wrapper.__click_params__.extend(func.__click_params__)
        except AttributeError:
            pass

        return click.command('MetricQ Dataheap importer for {}'.format(name))(wrapper)

    return decorator
