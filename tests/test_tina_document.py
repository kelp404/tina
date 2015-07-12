import unittest
from mock import MagicMock, patch


class TestTinaDocument(unittest.TestCase):
    def test_tina_document_get_by_id(self):
        with patch('tina.document.utils.get_elasticsearch', new=MagicMock()) as mock_es:
            from tina.document import Document
            Document.get_index_name = MagicMock(return_value='index_name')
            Document.get('id')
        mock_es().get.assert_called_once_with(
            index='index_name',
            doc_type='Document',
            id='id',
        )

    def test_tina_document_get_by_ids(self):
        with patch('tina.document.utils.get_elasticsearch', new=MagicMock()) as mock_es:
            from tina.document import Document
            Document.get_index_name = MagicMock(return_value='index_name')
            Document.get(['id-A'])
        mock_es().mget.assert_called_once_with(
            index='index_name',
            doc_type='Document',
            body={
                'ids': ['id-A']
            },
        )

    def test_tina_document_where(self):
        from tina.document import Document
        with patch('tina.document.Query', new=MagicMock()) as mock_query:
            query = Document.where('email', equal='kelp@rinse.io')
            self.assertTrue(isinstance(query, MagicMock))
        mock_query(Document).intersect.assert_called_once_with(
            'email',
            equal='kelp@rinse.io'
        )

    def test_tina_document_all(self):
        from tina.document import Document
        with patch('tina.document.Query', new=MagicMock(return_value='query')):
            query = Document.all()
            self.assertEqual(query, 'query')

    def test_tina_document_save(self):
        from tina.document import Document
        document = Document()
        document.get_index_name = MagicMock(return_value='index_name')
        with patch('tina.document.Document._es', new=MagicMock()) as mock_es:
            document.save()
        mock_es.index.assert_called_once_with(
            index='index_name',
            doc_type='Document',
            id=None,
            version=0,
            body={}
        )

    def test_tina_document_delete(self):
        from tina.document import Document
        document = Document(_id='byMQ-ULRSJ291RG_eEwSfQ', _version=1)
        document.get_index_name = MagicMock(return_value='index_name')
        with patch('tina.document.Document._es', new=MagicMock()) as mock_es:
            document.delete(synchronized=True)
        mock_es.delete.assert_called_once_with(
            index='index_name',
            id='byMQ-ULRSJ291RG_eEwSfQ',
            doc_type='Document'
        )
