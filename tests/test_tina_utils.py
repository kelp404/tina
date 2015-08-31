import unittest
from mock import patch, MagicMock
from tina import utils


class TestTinaUtils(unittest.TestCase):
    def test_tina_utils_get_elasticsearch(self):
        with patch('django.conf.settings.TINA_ELASTICSEARCH_URL', new='http://es:9200'):
            with patch('elasticsearch.Elasticsearch', new=MagicMock(return_value='es')) as mock_es:
                es = utils.get_elasticsearch()
                self.assertEqual(es, 'es')
            mock_es.assert_called_once_with('http://es:9200')
