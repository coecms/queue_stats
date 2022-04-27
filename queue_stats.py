#!/g/data/hh5/public/apps/nci_scripts/python-analysis3
# Copyright 2020 Scott Wales
# author: Scott Wales <scott.wales@unimelb.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
from datetime import datetime
import json
import sqlalchemy as sa
import time
import socket


def qstatq():
    r = subprocess.run(['/opt/pbs/default/bin/qstat','-Q','-f','-F','json'],
            text=True, capture_output=True)

    j = json.loads(r.stdout)

    time = datetime.utcfromtimestamp(j['timestamp'])

    records = []

    for q, r in j['Queue'].items():
        states = dict([s.split(':') for s in r['state_count'].split()])

        resources = r.get('resources_assigned', {})
        for res in ['jobfs','mem']:
            if res in resources:
                resources[res] = resources[res][:-2]
        resources = {k: int(v) for k,v in resources.items()}

        records.append({
            'time': time,
            'queue': q,
            'jobs_total': r['total_jobs'],
            'enabled': r['enabled'] == 'True',
            'started': r['started'] == 'True',
            'jobs_transit': states['Transit'],
            'jobs_queued': states['Queued'],
            'jobs_held': states['Held'],
            'jobs_waiting': states['Waiting'],
            'jobs_running': states['Running'],
            'jobs_exiting': states['Exiting'],
            'jobs_begun': states['Begun'],
            'total_jobfs_kb': resources.get('jobfs', None),
            'total_mem_kb': resources.get('mem', None),
            'total_mpiprocs': resources.get('mpiprocs', None),
            'total_ncpus': resources.get('ncpus', None),
            'total_nodect': resources.get('nodect', None),
            })

    return records


def qstatp(project):
    try:
        r = subprocess.run(['/g/data/hh5/public/apps/nci_scripts/uqstat', '-P', project, '-f', 'json'],
                text=True, capture_output=True)
        
        j = json.loads(r.stdout)
    except:
        return []

    time = datetime.utcnow() 

    records = []

    columns = ['project','user','name','queue','state','ncpus','walltime','su','mem_pct', 'cpu_pct', 'qtime','charge_rate','ncpus_by_mem','mem_request','mem_used']

    for jobid, r in j.items():
        rec = {c: r[c] for c in columns}
        rec['jobid'] = jobid
        rec['time'] = time

        records.append(rec)

    return records

def main():
    metadata = sa.MetaData()
    qstatp_table = sa.Table('qstatp', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('time', sa.DateTime),
            sa.Column('jobid', sa.String),
            sa.Column('project', sa.String),
            sa.Column('user', sa.String),
            sa.Column('name', sa.String),
            sa.Column('queue', sa.String),
            sa.Column('state', sa.String),
            sa.Column('ncpus', sa.Integer),
            sa.Column('walltime', sa.Integer),
            sa.Column('su', sa.Float),
            sa.Column('mem_pct', sa.Float),
            sa.Column('cpu_pct', sa.Float),
            sa.Column('qtime', sa.Integer),
            sa.Column('charge_rate', sa.Integer),
            sa.Column('ncpus_by_mem', sa.Float),
            sa.Column('mem_request', sa.Float),
            sa.Column('mem_used', sa.Float),
            )

    qstatq_table = sa.Table('qstatq', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('time', sa.DateTime),
            sa.Column('queue', sa.String),
            sa.Column('jobs_total', sa.Integer),
            sa.Column('enabled', sa.Boolean),
            sa.Column('started', sa.Boolean),
            sa.Column('jobs_transit', sa.Integer),
            sa.Column('jobs_queued', sa.Integer),
            sa.Column('jobs_held', sa.Integer),
            sa.Column('jobs_waiting', sa.Integer),
            sa.Column('jobs_running', sa.Integer),
            sa.Column('jobs_exiting', sa.Integer),
            sa.Column('jobs_begun', sa.Integer),
            sa.Column('total_jobfs_kb', sa.BigInteger),
            sa.Column('total_mem_kb', sa.BigInteger),
            sa.Column('total_mpiprocs', sa.Integer),
            sa.Column('total_ncpus', sa.Integer),
            sa.Column('total_nodect', sa.Integer),
            )

    with socket.socket() as s:
        s.bind(('',0))
        port = s.getsockname()[1]

    tunnel = subprocess.Popen(f'/bin/ssh -NL {port}:localhost:5432 jenkins'.split(' '))
    try:
        time.sleep(2)

        engine = sa.create_engine(f'postgresql://localhost:{port}/grafana')
        metadata.create_all(engine)

        conn = engine.connect()
        conn.execute(qstatq_table.insert(), qstatq())

        for p in ['w40', 'w42', 'w97', 'v45', 'x77', 'e14', 'g40']:
            print(p)
            s = qstatp(p)
            if len(s) > 0:
                conn.execute(qstatp_table.insert(), s)

    finally:
        tunnel.terminate()
        pass

if __name__ == '__main__':
    main()
