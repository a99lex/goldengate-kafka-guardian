import importlib.util
import pathlib
import unittest
from unittest.mock import patch
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location('guardian', pathlib.Path(__file__).parents[1] / 'collector/guardian.py')
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)

class CollectorTests(unittest.TestCase):
    def test_lag_unknown_and_retention(self):
        for committed in (-1001, 2, 200):
            self.assertIsNone(g.partition_row('g', 't', 0, committed, 10, 100)['lag'])
        self.assertEqual(g.partition_row('g', 't', 0, 60, 10, 100)['lag'], 40)

    def test_ogg_and_failed_source(self):
        deployment = {'name': 'LAB', 'url': 'https://gg.example', 'username_env': 'U', 'password_env': 'P', 'processes': [{'id': 'EXT1', 'kind': 'Extract'}, {'id': 'REP1', 'kind': 'Replicat'}]}
        with patch.dict('os.environ', {'U': 'user', 'P': 'password'}), patch.object(g, 'request_json', side_effect=[{'response': {'status': 'running', 'lag': 9}}, RuntimeError('secret error text')]):
            result = g.collect({'goldengate': [deployment]})
        self.assertEqual(result['processes'][0]['lag'], 9)
        self.assertEqual(result['processes'][1]['status'], 'Unavailable')
        self.assertIsNone(result['processes'][1]['lag'])
        self.assertNotIn('secret error', str(result))
        self.assertEqual(len(result['errors']), 1)

    def test_kafka_read_only_collection(self):
        seen = {}
        class TP:
            def __init__(self, topic, partition):
                self.topic, self.partition, self.offset, self.error = topic, partition, 60, None
        class Client:
            def __init__(self, config): seen.update(config)
            def list_topics(self, **kwargs):
                return SimpleNamespace(topics={'t': SimpleNamespace(error=None, partitions={0: None})})
            def committed(self, parts, **kwargs): return parts
            def get_watermark_offsets(self, partition, **kwargs): return (10, 100)
            def close(self): seen['closed'] = True
        fake = SimpleNamespace(Consumer=Client, TopicPartition=TP)
        result = {'partitions': [], 'errors': []}
        with patch.dict('sys.modules', {'confluent_kafka': fake}):
            g.collect_kafka({'client': {'security.protocol': 'SSL'}, 'groups': [{'id': 'g', 'topics': ['t']}]}, result)
        self.assertEqual(result['partitions'][0]['lag'], 40)
        self.assertFalse(seen['enable.auto.commit'])
        self.assertFalse(seen['allow.auto.create.topics'])
        self.assertTrue(seen['closed'])

    def test_https_and_redirect(self):
        for url in ('http://host', 'https://user:pass@host', 'https://host?token=x'):
            with self.assertRaises(ValueError): g.https_url(url)
        self.assertIsNone(g.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://other'))

if __name__ == '__main__': unittest.main()
