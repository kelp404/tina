from django.conf import settings
import elasticsearch


def get_elasticsearch():
    """
    Get the connection for ElasticSearch.
    :return: {Elasticsearch}
    """
    url = getattr(settings, 'TINA_ELASTICSEARCH_URL', 'http://localhost:9200')
    if url.startswith('https://'):
        import certifi
        return elasticsearch.Elasticsearch(
            url,
            verify_certs=True,
            ca_certs=certifi.where(),
        )
    else:
        return elasticsearch.Elasticsearch(url)

def get_index_prefix():
    """
    Get index prefix.
    :return: {string}
    """
    return getattr(settings, 'TINA_INDEX_PREFIX', '')
