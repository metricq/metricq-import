# metricq
# Copyright (C) 2019 ZIH,
# Technische Universitaet Dresden,
# Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq.
#
# metricq is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# metricq is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with metricq.  If not, see <http://www.gnu.org/licenses/>.


import asyncio
import tempfile
import json
import os
import datetime
import subprocess

import click
import cloudant
import pymysql

from metricq.logging import get_logger
from metricq.types import Timestamp
from metricq import Client

from .import_metric import ImportMetric

logger = get_logger()

logger.setLevel('INFO')


def em(text: str):
    return click.style(text, bold=True)


# if you know what I mean...
class FakeAgent(Client):
    async def connect(self):
        await super().connect()

        # unfortunately, the manager will take some time, if the number of metrics is quite high,
        # so let's increase the timeout.
        logger.warn("Sending db.register to manager, the response may take some time. Stay a while and listen...")
        await self.rpc('db.register', timeout=360)
        await self.stop()


class DataheapToHTAImporter(object):
    def __init__(self,
                 metricq_token, metricq_url,
                 couchdb_url: str, couchdb_user: str, couchdb_password: str,
                 import_workers: int,
                 import_host: str, import_port: int,
                 import_user: str,  import_password: str, import_database: str,
                 dry_run: bool = False,
                 check_values: bool = False,
                 check_interval: bool = True,
                 check_max_age=datetime.timedelta(hours=8),
                 assume_yes: bool = False):
        self._metricq_url = metricq_url
        self._metricq_token = metricq_token

        self._couchdb_client = cloudant.client.CouchDB(couchdb_user, couchdb_password,
                                                       url=couchdb_url, connect=True)
        self._couchdb_session = self._couchdb_client.session()
        self._couchdb_db_config = self._couchdb_client.create_database("config")
        self._couchdb_db_import = self._couchdb_client.create_database("import")

        self._num_workers = import_workers

        self._import_host = import_host
        self._import_port = import_port
        self._import_user = import_user
        self._import_password = import_password
        self._import_database = import_database

        self._metrics = []
        self._failed_imports = []

        self._last_metric = None
        self._import_begin = None

        self._dry_run = dry_run

        self._check_values = check_values
        self._check_interval = check_interval
        self._check_max_age = check_max_age
        self._assume_yes = assume_yes

        if not self._dry_run and not self._metricq_token:
            raise ValueError('Must specify metricq-token unless dry-run')

    def register(self, metricq_name, import_name, **kwargs):
        self._metrics.append(ImportMetric(metricq_name, import_name, **kwargs))

    def _confirm(self, text: str = 'continue?', abort: bool = True):
        if self._assume_yes:
            return True
        return click.confirm(text, abort=abort)

    @property
    def import_metrics(self):
        return [metric for metric in self._metrics if metric.import_name]

    @property
    def metrics(self):
        return self._metrics

    def run(self):
        if not self._metrics:
            raise ValueError('No metrics defined')
        if self._dry_run:
            return self.dry_run()
        return self.real_run()

    def real_run(self):
        assert self._import_begin is None
        self._confirm(f'Please make sure the MetricQ db with the token '
                      f'"{self._metricq_token}" is not running! Continue?')

        self._update_config()
        self._create_bindings()
        self._import_begin = Timestamp.now()
        self._run_import()

        if self._failed_imports:
            print('The following metrics have failed to import:')
            for metric in self._failed_imports:
                print(f' - {metric.metricq_name}')

    def dry_run(self):
        mysql = pymysql.connect(host=self._import_host, port=self._import_port,
                                user=self._import_user, passwd=self._import_password,
                                db=self._import_database)
        counts = {}
        for metric in self._metrics:
            with mysql.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) as count, "
                               f"MIN(timestamp) as t_min, MAX(timestamp) as t_max"
                               f" FROM `{metric.import_name}`")
                count, t_min, t_max = cursor.fetchone()
                t_min /= 1e3
                t_max /= 1e3
                click.echo(f'{em(metric.metricq_name):30s} <== {metric.import_name:30s} with {count:14,} entries')
                click.echo(f'        intervals {metric.interval_min:,}, {metric.interval_max:,}, {metric.interval_factor}')

                dt_min = datetime.datetime.fromtimestamp(t_min)
                dt_max = datetime.datetime.fromtimestamp(t_max)
                interval_avg = (t_max - t_min) / count
                click.echo(f'        time range {dt_min} - {dt_max}, avg interval {interval_avg}')
                delta = datetime.datetime.now() - dt_max
                if self._check_max_age and not (datetime.timedelta() < delta < self._check_max_age):
                    click.secho(f'suspicious max time {delta}', bg='red', bold=True)
                    self._confirm()

                expected_interval = 1 / metric.sampling_rate
                tolerance = 1.5
                if self._check_interval and not (expected_interval / tolerance < interval_avg < expected_interval * tolerance):
                    click.secho(f'suspicious interval, expected {expected_interval}', bg='red', bold=True)
                    self._confirm()

                if self._check_values:
                    cursor.execute(f"MIN(value) as value_min, MAX(value) as value_max, "
                                   f" FROM `{metric.import_name}`")
                    value_min, value_max = cursor.fetchone()

                    click.echo(f'        value range {value_min} to {value_max}')
                    if not (-1e9 < value_min < value_max < 1e9):
                        click.secho('suspicious value range', bg='red', bold=True)
                        self._confirm()

                click.echo()
                counts[metric.metricq_name] = count
        assert(len(counts) == len(self._metrics))
        count_total = sum(counts.values())
        count_mean = int(count_total / len(counts))
        count_min = min(counts.values())
        count_max = max(counts.values())
        click.echo(f'Total: {count_total:14,}, average {count_mean:14,} per metric')
        click.echo(f'Range: {count_min:14,} - {count_max:14,}')

    def _update_config(self):
        config_metrics = {metric.metricq_name: metric.config for metric in self._metrics}

        try:
            config_document = self._couchdb_db_config[self._metricq_token]
        except KeyError:
            raise KeyError(f"Please add a basic configuration in the CouchDB for the db '{self._metricq_token}'")

        config_document.fetch()
        current_config = dict(config_document)
        current_metrics = current_config["metrics"]
        current_metric_names = list(current_metrics.keys())
        conflicting_metrics = [metric for metric in self._metrics if metric.metricq_name in current_metric_names]

        if conflicting_metrics:
            print('The following metrics have already been defined in the database, but are in the import set:')
            for metric in conflicting_metrics:
                print(f' - {metric.metricq_name}')

            if not self._confirm('Overwrite old entries?', abort=False):
                raise RuntimeError('Please remove the config for the existing metrics'
                                   ' or remove them from the import set')

        current_metrics.update(config_metrics)

        config_document['metrics'] = current_metrics
        config_document.save()

        self.import_config = {
            'type': "file",
            'threads': 2,
            'path': current_config['path'],
            'import': {
                'host': self._import_host,
                'user': self._import_user,
                'password': self._import_password,
                'database': self._import_database
            }
        }

    def _create_bindings(self):
        fake_agent = FakeAgent(self._metricq_token, self._metricq_url)
        fake_agent.run()

    def _show_last_metric(self, item):
        if self._last_metric:
            return self._last_metric.metricq_name
        return ''

    def _run_import(self):
        # setup task queue
        self.queue = asyncio.Queue()
        for metric in self.import_metrics:
            self.queue.put_nowait(metric)
        self.num_import_metrics = self.queue.qsize()

        # run all pending import tasks
        with click.progressbar(length=self.num_import_metrics, label='Importing metrics', item_show_func=self._show_last_metric) as bar:
            asyncio.run(self.import_main(bar))

    async def import_worker(self, bar):
        while True:
            try:
                metric = self.queue.get_nowait()
                self._last_metric = metric
            except asyncio.QueueEmpty:
                return
            await self.import_metric(metric)
            bar.update(1)

    async def import_main(self, bar):
        workers = [self.import_worker(bar) for _ in range(self._num_workers)]
        await asyncio.wait(workers)

    async def import_metric(self, metric):
        # write config into a tmpfile
        conffile, conffile_name = tempfile.mkstemp(prefix='metricq-import-', suffix='.json', text=True)
        with open(conffile, 'w') as conf:
            config = self.import_config.copy()
            config['metrics'] = []
            metric_config = metric.config
            metric_config['name'] = metric.metricq_name
            config['metrics'].append(metric_config)
            json.dump(config, conf)

        args = ("hta_mysql_import",
                '-m', metric.metricq_name,
                '--import-metric', metric.import_name,
                '-c', conffile_name,
                '--max-timestamp', str(int(self._import_begin.posix_ms)))

        import_data = {
            '_id': metric.metricq_name,
            'import_name': metric.import_name,
            'dataheap_name': metric.dataheap_name,
            'begin': datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat(),
            'arguments': args,
            'config': config,
        }

        import_doc = self._couchdb_db_import.create_document(import_data)
        import_doc.save()

        try:
            process = await asyncio.create_subprocess_exec(*args, stdout=subprocess.PIPE)

            await process.communicate()

            import_doc['return_code'] = process.returncode
            import_doc['end'] = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
            import_doc.save()

            if process.returncode != 0:
                self._failed_imports.append(metric)
        except FileNotFoundError:
            logger.error('Make sure hta_mysql_import is in your PATH.')

        try:
            os.remove(conffile_name)
            pass
        except OSError:
            pass
