#!/usr/bin/env python3
"""Check the project's Helm contract without accessing a Kubernetes cluster."""
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
HELM = os.environ.get('HELM', 'helm')
CHART = str(ROOT / 'deploy/helm/gpu-telemetry')

def render(*args, succeeds=True):
    result = subprocess.run([HELM, 'template', 'check', CHART, *args],
                            text=True, capture_output=True, check=False)
    assert (result.returncode == 0) == succeeds, result.stderr
    return result.stdout

plain = render()
for removed in ('secretKeyRef:', 'kind: Secret', 'TLS_CERT_FILE', 'TLS_KEY_FILE',
                'TLS_CA_FILE', 'INSECURE_TRANSPORT', 'ALLOW_INSECURE_DATABASE',
                'POSTGRES_PASSWORD', 'COLLECTOR_PASSWORD', 'GATEWAY_PASSWORD',
                'init-roles.sh', 'kind: NetworkPolicy'):
    assert removed not in plain, f'Out-of-scope flow still rendered: {removed}'
assert 'name: POSTGRES_HOST_AUTH_METHOD, value: trust' in plain
assert 'name: QUEUE_MAX_BYTES, value: "67108864"' in plain
large = render('--set', 'queue.maxBytes=1099511627776')
assert 'name: QUEUE_MAX_BYTES, value: "1099511627776"' in large
assert plain.count('postgres://postgres@check-postgres:5432/telemetry?sslmode=disable') == 3
assert re.search(r'name: COLLECTOR_SLOTS, value: "1"', plain)
assert plain.count('name: MAX_MESSAGE_BYTES,') == 3
assert plain.count('name: ACK_TIMEOUT,') == 1
assert plain.count('name: QUEUE_MAX_SLOTS,') == 1
assert plain.count('name: QUEUE_URL,') == 2
message_limit = render('--set', 'queue.maxMessageBytes=131072')
assert message_limit.count('name: MAX_MESSAGE_BYTES, value: "131072"') == 3
assert len(re.findall(r'^kind: Deployment$', plain, re.M)) == 4
assert len(re.findall(r'^kind: StatefulSet$', plain, re.M)) == 1
slots = render('--set', 'collector.slots=4')
assert 'name: COLLECTOR_SLOTS, value: "4"' in slots
render('--set', 'queue.replicas=2', succeeds=False)
render('--set', 'collector.slots=129', succeeds=False)
render('--set', 'database.postgresEnabled=false', succeeds=False)
render('--set', 'security.insecureTransport=true', succeeds=False)
render('--set', 'networkPolicy.enabled=true', succeeds=False)
external = render('--set', 'database.postgresEnabled=false', '--set-string',
                  'database.url=postgres://postgres@external:5432/telemetry?sslmode=disable')
assert 'kind: StatefulSet' not in external
assert external.count('postgres://postgres@external:5432/telemetry?sslmode=disable') == 3
print('PASS: password-free chart, one instance defaults, configured slots, external database URL, no TLS/Secret flows, invalid values rejected.')
