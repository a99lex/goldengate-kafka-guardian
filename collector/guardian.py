#!/usr/bin/env python3
"""Read-only GoldenGate/Kafka collector. No consume, start, stop or writes to sources."""
import argparse
import base64
import datetime as dt
import json
import logging
import os
import ssl
import time
import urllib.parse
import urllib.request


def secret(name):
    value = os.environ.get(name, '')
    if not value:
        raise ValueError('Missing secret environment variable')
    return value


def https_url(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError('Use an HTTPS URL without embedded credentials or query')
    return url.rstrip('/')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward credentials to a redirect target.


def request_json(url, headers, data=None, ca_file=None):
    context = ssl.create_default_context(cafile=ca_file)
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), NoRedirect())
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers=headers)
    with opener.open(req, timeout=15) as response:
        return json.load(response)


def collect_ogg(deployment, snapshot):
    name = deployment['name']
    base = https_url(deployment['url'])
    credentials = secret(deployment['username_env']) + ':' + secret(deployment['password_env'])
    headers = {'Authorization': 'Basic ' + base64.b64encode(credentials.encode()).decode()}
    for process in deployment['processes']:
        kind, process_id = process['kind'], process['id']
        if kind not in ('Extract', 'Replicat'):
            raise ValueError('Invalid process kind')
        row = {'id': process_id, 'kind': kind, 'deployment': name, 'status': 'Unavailable', 'lag': None}
        try:
            collection = 'extracts' if kind == 'Extract' else 'replicats'
            url = f'{base}/services/v2/{collection}/{urllib.parse.quote(process_id, safe="")}/info/status'
            result = request_json(url, headers, ca_file=deployment.get('ca_file'))['response']
            row['status'] = result['status'].capitalize()
            lag = result.get('lag')
            row['lag'] = lag if isinstance(lag, (int, float)) and lag >= 0 else None
        except Exception:
            snapshot['errors'].append({'source': f'{name}/{process_id}', 'message': 'GoldenGate read failed; check URL, TLS, credentials and process name'})
        snapshot['processes'].append(row)


def partition_row(group, topic, partition, committed, low, high):
    # Negative committed offsets mean no committed position; never report zero lag.
    valid = committed >= low and committed <= high and committed >= 0
    return {'group': group, 'topic': topic, 'partition': partition,
            'committed': committed if committed >= 0 else None,
            'endOffset': high, 'lag': high - committed if valid else None}


def collect_kafka(config, snapshot):
    from confluent_kafka import Consumer, TopicPartition
    common = dict(config['client'])
    for setting, env_name in config.get('secret_env', {}).items():
        common[setting] = secret(env_name)
    if common.get('security.protocol') not in ('SSL', 'SASL_SSL'):
        raise ValueError('Kafka SSL or SASL_SSL required')
    if common.get('enable.ssl.certificate.verification') is False:
        raise ValueError('Kafka certificate verification must be enabled')
    for group in config['groups']:
        # Never subscribe/poll: only metadata, committed offsets and watermarks.
        client = Consumer({**common, 'group.id': group['id'], 'enable.auto.commit': False,
                           'enable.auto.offset.store': False, 'allow.auto.create.topics': False})
        try:
            for topic in group['topics']:
                try:
                    metadata = client.list_topics(topic=topic, timeout=10).topics[topic]
                    if metadata.error:
                        raise RuntimeError('Topic metadata unavailable')
                    partitions = [TopicPartition(topic, number) for number in metadata.partitions]
                    offsets = client.committed(partitions, timeout=10)
                    for partition in offsets:
                        if partition.error:
                            raise RuntimeError('Partition offset unavailable')
                        low, high = client.get_watermark_offsets(partition, timeout=10, cached=False)
                        snapshot['partitions'].append(partition_row(group['id'], topic, partition.partition, partition.offset, low, high))
                except Exception:
                    snapshot['errors'].append({'source': f"kafka/{group['id']}/{topic}", 'message': 'Kafka read failed; check broker access, ACLs, TLS and group configuration'})
        finally:
            client.close()


def collect(config):
    snapshot = {'schemaVersion': 1, 'collectorId': config.get('collector_id', 'default'),
                'collectedAt': '', 'processes': [], 'partitions': [], 'errors': []}
    for deployment in config.get('goldengate', []):
        try:
            collect_ogg(deployment, snapshot)
        except Exception:
            snapshot['errors'].append({'source': deployment['name'], 'message': 'GoldenGate collector configuration failed'})
    if config.get('kafka'):
        try:
            collect_kafka(config['kafka'], snapshot)
        except Exception:
            snapshot['errors'].append({'source': 'kafka', 'message': 'Kafka collector configuration or client initialization failed'})
    # Stamp collection completion; individual requests have bounded timeouts.
    snapshot['collectedAt'] = dt.datetime.now(dt.timezone.utc).isoformat()
    return snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--dry-run', action='store_true', help='Print snapshot without uploading (may contain infrastructure names)')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    with open(args.config) as handle:
        config = json.load(handle)
    endpoint = https_url(config['guardian_url']) + '/api/ingest'
    interval = max(30, int(config.get('interval_seconds', 60)))
    while True:
        started = time.monotonic()
        snapshot = collect(config)
        if args.dry_run:
            print(json.dumps(snapshot, indent=2))
            return
        try:
            request_json(endpoint, {'Authorization': 'Bearer ' + secret('GUARDIAN_INGEST_TOKEN'), 'Content-Type': 'application/json'}, snapshot)
            logging.info('Snapshot sent; source errors=%d', len(snapshot['errors']))
        except Exception:
            logging.error('Upload failed; check endpoint, token and network (credentials omitted)')
            if args.once:
                raise SystemExit(1)
        if args.once:
            return
        time.sleep(max(1, interval - (time.monotonic() - started)))


if __name__ == '__main__':
    main()
