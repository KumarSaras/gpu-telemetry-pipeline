#!/usr/bin/env python3
"""Verify local images through Helm in a disposable, uniquely named Kind cluster.

Every cluster operation uses a separate kubeconfig. Cleanup removes only the
cluster created by this run; existing clusters and database volumes are untouched.
"""
from datetime import datetime
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
SERVICES = ('queue', 'streamer', 'collector', 'gateway', 'migrate')


def main():
    executables = {name: os.environ.get(name.upper(), name)
                   for name in ('kind', 'kubectl', 'helm', 'docker')}
    for name, executable in executables.items():
        if not shutil.which(executable):
            raise RuntimeError(f'{name} is required for the Kind/Helm smoke test')
    prefix = os.environ.get('IMAGE_PREFIX', 'gpu-telemetry')
    tag = os.environ.get('IMAGE_TAG', 'dev')
    images = [f'{prefix}/{service}:{tag}' for service in SERVICES]
    cluster = 'gpu-telemetry-smoke-' + secrets.token_hex(4)
    release, namespace = 'smoke', 'gpu-telemetry-smoke'
    cache = ROOT / '.cache'
    cache.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='kind-smoke-', dir=cache) as directory:
        directory = Path(directory)
        kubeconfig = directory / 'kubeconfig'
        env = {**os.environ, 'KUBECONFIG': str(kubeconfig),
               'KIND_EXPERIMENTAL_PROVIDER': 'docker'}
        forwarders = []
        created = False

        def run(name, *args, timeout=60, check=True):
            result = subprocess.run([executables[name], *args], cwd=ROOT,
                                    env=env, text=True, capture_output=True,
                                    timeout=timeout, check=False)
            if check and result.returncode:
                raise RuntimeError(f'{name} {args[0]} failed: '
                                   + (result.stdout + result.stderr)[-4000:])
            return result.stdout

        def kubectl(*args, **kwargs):
            return run('kubectl', '--kubeconfig', str(kubeconfig),
                       '--namespace', namespace, *args, **kwargs)

        def forward(resource, port):
            output = directory / f'forward-{port}.log'
            stream = output.open('w')
            try:
                process = subprocess.Popen(
                    [executables['kubectl'], '--kubeconfig', str(kubeconfig),
                     '--namespace', namespace, 'port-forward', resource,
                     f':{port}', '--address', '127.0.0.1'], cwd=ROOT, env=env,
                    text=True, stdout=stream, stderr=subprocess.STDOUT)
            finally:
                stream.close()
            forwarders.append(process)
            deadline = time.monotonic() + 30
            while True:
                log = output.read_text()
                match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+) ->', log)
                if match:
                    return 'http://127.0.0.1:' + match[1]
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('Port forwarding failed: ' + log[-2000:])
                time.sleep(0.1)

        def text(base, path):
            with urllib.request.urlopen(base + path, timeout=10) as response:
                return response.read().decode()

        def metrics(base):
            return {line.split()[0]: float(line.split()[1])
                    for line in text(base, '/metrics').splitlines()
                    if line and not line.startswith('#')}

        def row_count():
            return int(kubectl('exec', f'statefulset/{release}-postgres', '--',
                               'psql', '-U', 'postgres', '-d', 'telemetry',
                               '-Atc', 'SELECT count(*) FROM telemetry').strip())

        def upgrade(*settings):
            args = ['upgrade', release, str(ROOT / 'deploy/helm/gpu-telemetry'),
                    '--kubeconfig', str(kubeconfig), '--namespace', namespace,
                    '--reuse-values']
            for setting in settings:
                args.extend(('--set', setting))
            run('helm', *args, '--wait', '--wait-for-jobs', '--timeout', '5m',
                timeout=330)

        try:
            # Fail before creating resources if the caller has not built images.
            run('docker', 'image', 'inspect', *images)
            if cluster in run('kind', 'get', 'clusters').splitlines():
                raise RuntimeError('Refusing to reuse an existing smoke cluster')
            print(f'Creating isolated Kind cluster {cluster}', flush=True)
            created = True  # Include cleanup for a partially created cluster.
            run('kind', 'create', 'cluster', '--name', cluster, '--kubeconfig',
                str(kubeconfig), '--wait', '120s', timeout=240)
            run('kind', 'load', 'docker-image', *images, '--name', cluster,
                timeout=240)
            print('Installing the application through Helm', flush=True)
            run('helm', 'upgrade', '--install', release,
                str(ROOT / 'deploy/helm/gpu-telemetry'), '--kubeconfig',
                str(kubeconfig), '--namespace', namespace, '--create-namespace',
                '--set-string', f'images.prefix={prefix}', '--set-string',
                f'images.tag={tag}', '--set', 'streamer.interval=5ms',
                '--wait', '--wait-for-jobs', '--timeout', '5m', timeout=330)
            deployments = json.loads(kubectl('get', 'deployments', '-o', 'json'))['items']
            assert len(deployments) == 4
            assert not json.loads(kubectl('get', 'networkpolicies', '-o', 'json'))['items']
            assert all(d['spec']['replicas'] == 1 and
                       d['status'].get('readyReplicas') == 1 for d in deployments)
            queue_selector = f'app.kubernetes.io/instance={release},app.kubernetes.io/component=queue'
            queue_uid = kubectl('get', 'pods', '-l', queue_selector,
                                '-o', 'jsonpath={.items[0].metadata.uid}')
            collector = next(d for d in deployments if d['metadata']['name'] == release + '-collector')
            variables = collector['spec']['template']['spec']['containers'][0]['env']
            assert next(v['value'] for v in variables if v['name'] == 'COLLECTOR_SLOTS') == '1'
            endpoints = {}
            for service, port in [('gateway', 8080), ('queue', 8081),
                                  ('collector', 8082), ('streamer', 8083)]:
                base = forward(f'deployment/{release}-{service}', port)
                endpoints[service] = base
                assert text(base, '/livez').strip() == 'ok'
                assert text(base, '/readyz').strip() == 'ready'
                observed = metrics(base)
                assert observed.get('gpu_telemetry_http_requests_total', 0) >= 1
                assert 'gpu_telemetry_http_request_seconds_sum' in observed
            print('Health and HTTP metrics verified on all four applications', flush=True)
            deadline = time.monotonic() + 90
            while row_count() < 2470:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Timed out waiting for a full fixture replay')
                time.sleep(0.2)
            # The chart replays continuously. Pause only this test's streamer,
            # then drain the queue so database/API totals describe one snapshot.
            streamer_pods = kubectl('get', 'pods', '-l',
                                    f'app.kubernetes.io/instance={release},app.kubernetes.io/component=streamer',
                                    '-o', 'name').splitlines()
            upgrade('streamer.replicas=0')
            if streamer_pods:
                kubectl('wait', '--for=delete', *streamer_pods,
                        '--timeout=60s', timeout=70)
            deadline = time.monotonic() + 60
            previous = None
            while True:
                observed = metrics(endpoints['queue'])
                count = row_count()
                if observed['gpu_telemetry_queue_messages'] == 0 and count == previous:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('Queue did not drain after pausing the streamer')
                previous = count
                time.sleep(0.2)
            assert count >= 2470
            assert observed['gpu_telemetry_rejected_total'] == 0
            assert observed['gpu_telemetry_subscribers'] == 1
            assert observed['gpu_telemetry_subscriber_slots'] == 1
            gateway = endpoints['gateway']
            ids = json.loads(text(gateway, '/api/v1/gpus'))
            assert len(ids) == 247 and len(set(ids)) == 247
            total = 0
            first = None
            for gpu in ids:
                path = '/api/v1/gpus/' + urllib.parse.quote(gpu, safe='') + '/telemetry'
                rows = json.loads(text(gateway, path))
                assert len(rows) >= 10 and all(row['uuid'] == gpu for row in rows)
                timestamps = [datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                              for row in rows]
                assert timestamps == sorted(timestamps)
                total += len(rows)
                if first is None:
                    first = (path, rows[0]['timestamp'])
            assert total == count
            path, timestamp = first
            query = urllib.parse.urlencode({'start_time': timestamp, 'end_time': timestamp})
            bounded = json.loads(text(gateway, path + '?' + query))
            assert bounded and all(row['timestamp'] == timestamp for row in bounded)
            assert len(json.loads(text(gateway, '/openapi.json'))['paths']) == 2
            collector_pods = kubectl('get', 'pods', '-l',
                                     f'app.kubernetes.io/instance={release},app.kubernetes.io/component=collector',
                                     '-o', 'name').splitlines()
            upgrade('collector.replicas=0')
            if collector_pods:
                kubectl('wait', '--for=delete', *collector_pods,
                        '--timeout=60s', timeout=70)
            paused = json.loads(kubectl('get', 'deployments', '-o', 'json'))['items']
            assert all(d['spec']['replicas'] == 0 for d in paused
                       if d['metadata']['name'] in (release + '-streamer', release + '-collector'))
            deadline = time.monotonic() + 30
            while metrics(endpoints['queue'])['gpu_telemetry_subscribers'] != 0:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Collector subscription remained after scale-to-zero')
                time.sleep(0.2)
            assert row_count() == count
            upgrade('collector.replicas=1', 'streamer.replicas=1')
            deadline = time.monotonic() + 60
            while row_count() <= count:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Ingestion did not resume after Helm scale-up')
                time.sleep(0.2)
            assert kubectl('get', 'pods', '-l', queue_selector,
                           '-o', 'jsonpath={.items[0].metadata.uid}') == queue_uid
            print(f'PASS: Kind/Helm install, one instance/slot, all health/HTTP metrics, '
                  f'{count} committed/API records after full replay and drain, '
                  '247 GPUs, ordered telemetry and inclusive filters, '
                  'Helm scale-to-zero and resume without queue restart.', flush=True)
        except Exception:
            if created and kubeconfig.exists():
                for args in [('get', 'pods,pvc'), ('get', 'events', '--sort-by=.lastTimestamp')]:
                    print(kubectl(*args, timeout=15, check=False)[-4000:])
                for service in ('queue', 'collector', 'gateway', 'streamer'):
                    print(kubectl('logs', f'deployment/{release}-{service}',
                                  '--tail=20', timeout=15, check=False)[-2000:])
            raise
        finally:
            for process in forwarders:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            if created:
                run('kind', 'delete', 'cluster', '--name', cluster,
                    '--kubeconfig', str(kubeconfig), timeout=120)
                print(f'Removed test cluster {cluster} and its test data', flush=True)


if __name__ == '__main__':
    main()
