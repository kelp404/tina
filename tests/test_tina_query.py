import unittest
from mock import MagicMock, patch
from tina.query import QueryOperation, QueryCell, Query
from tina.document import Document
from tina.properties import StringProperty, DateTimeProperty
from tina.exceptions import QuerySyntaxError


class TestTinaQueryOperation(unittest.TestCase):
    def test_tina_query_operation(self):
        self.assertEqual(QueryOperation.normal_operation_mask, 0x3F)
        self.assertEqual(QueryOperation.unequal, 0x000)
        self.assertEqual(QueryOperation.equal, 0x001)
        self.assertEqual(QueryOperation.less, 0x002)
        self.assertEqual(QueryOperation.less_equal, 0x003)
        self.assertEqual(QueryOperation.greater, 0x004)
        self.assertEqual(QueryOperation.greater_equal, 0x005)
        self.assertEqual(QueryOperation.like, 0x011)
        self.assertEqual(QueryOperation.unlike, 0x010)
        self.assertEqual(QueryOperation.contains, 0x021)
        self.assertEqual(QueryOperation.exclude, 0x020)

        self.assertEqual(QueryOperation.intersection, 0x040)
        self.assertEqual(QueryOperation.union, 0x080)
        self.assertEqual(QueryOperation.all, 0x100)
        self.assertEqual(QueryOperation.order_asc, 0x600)
        self.assertEqual(QueryOperation.order_desc, 0x400)


class TesttinaQueryCell(unittest.TestCase):
    def test_tina_query_cell_only_operation(self):
        query = QueryCell(QueryOperation.all)
        self.assertIsNone(query.member)
        self.assertEqual(query.operation, QueryOperation.all)
        self.assertIsNone(query.value)
        self.assertIsNone(query.sub_queries)

    def test_tina_query_cell(self):
        query = QueryCell(
            QueryOperation.equal,
            'member',
            'value',
            sub_queries=[]
        )
        self.assertEqual(query.member, 'member')
        self.assertEqual(query.operation, QueryOperation.equal)
        self.assertEqual(query.value, 'value')
        self.assertListEqual(query.sub_queries, [])


class FakeDocument(Document):
    name = StringProperty()
    nickname = StringProperty()
    time = DateTimeProperty()
class TesttinaQuery(unittest.TestCase):
    def setUp(self):
        self.query = Query(FakeDocument)

    def test_tina_query(self):
        self.assertIs(self.query.document_class, FakeDocument)
        self.assertEqual(len(self.query.items), 1)
        self.assertEqual(self.query.items[0].operation, QueryOperation.all)

    def test_tina_query_where(self):
        self.query.intersect = MagicMock()
        self.query.where('email', equal='kelp@rinse.io')
        self.query.intersect.assert_called_once_with('email', equal='kelp@rinse.io')

    def test_tina_query_fetch(self):
        fake_es = MagicMock()
        fake_es().search.return_value = {
            'hits': {
                'hits': [],
                'total': 0
            }
        }
        with patch('tina.document.Document._es', new=fake_es):
            self.query.document_class.get_index_name = MagicMock(return_value='index_name')
            self.query.fetch()
        fake_es.search.assert_called_once_with(
            index='index_name',
            body={'sort': [], 'fields': ['_source'], 'from': 0, 'size': 1000},
            version=True,
        )

    def test_tina_query_first_none(self):
        self.query.fetch = MagicMock()
        self.query.fetch.return_value = tuple([[], 0])
        item = self.query.first()
        self.query.fetch.assert_called_once_with(1, 0, fetch_reference=True)
        self.assertIsNone(item)
    def test_tina_query_first(self):
        self.query.fetch = MagicMock()
        self.query.fetch.return_value = tuple([[{'_id': '4689f7addaedc3d52a9688722c3e595b', '_rev': '1-4689f7addaedc3d52a9688722c3e595b'}], 1])
        item = self.query.first()
        self.query.fetch.assert_called_once_with(1, 0, fetch_reference=True)
        self.assertDictEqual(item, {
            '_id': '4689f7addaedc3d52a9688722c3e595b',
            '_rev': '1-4689f7addaedc3d52a9688722c3e595b',
        })

    def test_tina_query__generate_elasticsearch_search_body_all(self):
        query = self.query.order_by('time')
        result = self.query._Query__generate_elasticsearch_search_body(query.items, 1000, 0)
        self.assertDictEqual(result, {
            'sort': [{'time': {'order': 'asc', 'ignore_unmapped': True, 'missing': '_first'}}], 'fields': ['_source'], 'from': 0, 'size': 1000
        })
    def test_tina_query__generate_elasticsearch_search_body_where(self):
        query = self.query.where('name', equal='kelp')
        result = self.query._Query__generate_elasticsearch_search_body(query.items, 1000, 0)
        self.assertDictEqual(result, {
            'sort': [], 'fields': ['_source'], 'from': 0, 'size': 1000,
            'query': {'bool': {'minimum_should_match': 1,
                               'should': [{'bool': {'minimum_should_match': 1,'should': [{'match': {'name': {'operator': 'and', 'query': 'kelp'}}}]}}]}},
        })

    def test_tina_query__compile_queries_order_by_asc(self):
        query = self.query.order_by('time')
        es_query, sort_list = self.query._Query__compile_queries(query.items)
        self.assertListEqual(sort_list, [
            {
                'time': {
                    'order': 'asc',
                    'ignore_unmapped': True,
                    'missing': '_first',
                }
            }
        ])
    def test_tina_query__compile_queries_order_by_desc(self):
        query = self.query.order_by('time', descending=True)
        es_query, sort_list = self.query._Query__compile_queries(query.items)
        self.assertListEqual(sort_list, [
            {
                'time': {
                    'order': 'desc',
                    'ignore_unmapped': True,
                    'missing': '_last',
                }
            }
        ])
    def test_tina_query__compile_queries_intersection(self):
        query = self.query.where('name', equal='kelp')
        es_query, sort_list = self.query._Query__compile_queries(query.items)
        self.assertDictEqual(es_query, {
            'bool': {
                'minimum_should_match': 1,
                'should': [{
                    'bool': {
                        'minimum_should_match': 1,
                        'should': [{
                            'match': {
                                'name': {
                                    'operator': 'and',
                                    'query': 'kelp'
                                }
                            }
                        }]
                    }
                }]
            }
        })
        self.assertListEqual(sort_list, [])
    def test_tina_query__compile_queries_union(self):
        query = self.query.where(lambda x:
                                 x.where('name', equal='kelp')\
                                 .union('nickname', equal='kelp')
        )
        es_query, sort_list = self.query._Query__compile_queries(query.items)
        self.assertDictEqual(es_query, {
            'bool': {
                'minimum_should_match': 1,
                'should': [{
                    'bool': {
                        'minimum_should_match': 1,
                        'should': [{
                            'bool': {
                                'minimum_should_match': 1,
                                'should': [{
                                    'match': {
                                        'name': {
                                            'operator': 'and',
                                            'query': 'kelp'
                                        }
                                    }
                                }, {
                                    'match': {
                                        'nickname': {
                                            'operator': 'and',
                                            'query': 'kelp'
                                        }
                                    }
                                }]
                            }
                        }]
                    }
                }]
            }
        })
        self.assertListEqual(sort_list, [])

    def test_tina_query__compile_query_like(self):
        query_cell = QueryCell(
            QueryOperation.like,
            member='email',
            value='kelp@rinse.io',
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'bool': {
                'should': [
                    {
                        'match': {
                            query_cell.member: {
                                'query': query_cell.value,
                                'operator': 'and',
                            }
                        }
                    },
                    {
                        'regexp': {
                            query_cell.member: '.*%s.*' % query_cell.value
                        }
                    },
                ]
            }
        })
    def test_tina_query__compile_query_unlike(self):
        query_cell = QueryCell(
            QueryOperation.unlike,
            member='email',
            value='kelp@rinse.io',
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'bool': {
                    'minimum_should_match': 2,
                    'should': [
                        {
                            'bool': {
                                'must_not': {
                                    'match': {
                                        query_cell.member: {
                                            'query': query_cell.value,
                                            'operator': 'and',
                                        }
                                    }
                                }
                            }
                        },
                        {
                            'bool': {
                                'must_not': {
                                    'regexp': {
                                        query_cell.member: '.*%s.*' % query_cell.value
                                    }
                                }
                            }
                        },
                    ]
                }
        })
    def test_tina_query__compile_query_contains(self):
        query_cell = QueryCell(
            QueryOperation.contains,
            member='email',
            value=['kelp'],
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'bool': {
                'should': [{'match': {query_cell.member: {'query': x, 'operator': 'and'}}} for x in query_cell.value],
            }
        })
    def test_tina_query__compile_query_contains_none(self):
        query_cell = QueryCell(
            QueryOperation.contains,
            member='email',
            value=[],
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {'bool': {'should': []}})
    def test_tina_query__compile_query_greater_equal(self):
        query_cell = QueryCell(
            QueryOperation.greater_equal,
            member='age',
            value=12,
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'range': {
                query_cell.member: {
                    'gte': query_cell.value
                }
            }
        })
    def test_tina_query__compile_query_greater(self):
        query_cell = QueryCell(
            QueryOperation.greater,
            member='age',
            value=12,
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'range': {
                query_cell.member: {
                    'gt': query_cell.value
                }
            }
        })
    def test_tina_query__compile_query_less_equal(self):
        query_cell = QueryCell(
            QueryOperation.less_equal,
            member='age',
            value=12,
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'range': {
                query_cell.member: {
                    'lte': query_cell.value
                }
            }
        })
    def test_tina_query__compile_query_less(self):
        query_cell = QueryCell(
            QueryOperation.less,
            member='age',
            value=12,
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'range': {
                query_cell.member: {
                    'lt': query_cell.value
                }
            }
        })
    def test_tina_query__compile_query_equal(self):
        query_cell = QueryCell(
            QueryOperation.equal,
            member='name',
            value='kelp',
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'match': {
                query_cell.member: {
                    'query': query_cell.value,
                    'operator': 'and',
                }
            }
        })
    def test_tina_query__compile_query_unequal(self):
        query_cell = QueryCell(
            QueryOperation.unequal,
            member='name',
            value='kelp',
        )
        result = self.query._Query__compile_query(query_cell)
        self.assertDictEqual(result, {
            'bool': {
                'must_not': {
                    'match': {
                        query_cell.member: {
                            'query': query_cell.value,
                            'operator': 'and',
                        }
                    }
                }
            }
        })

    def test_tina_query__parse_operation_equal(self):
        operation, value = self.query._Query__parse_operation(equal='equal')
        self.assertEqual(operation, QueryOperation.equal)
        self.assertEqual(value, 'equal')
    def test_tina_query__parse_operation_unequal(self):
        operation, value = self.query._Query__parse_operation(unequal='unequal')
        self.assertEqual(operation, QueryOperation.unequal)
        self.assertEqual(value, 'unequal')
    def test_tina_query__parse_operation_less(self):
        operation, value = self.query._Query__parse_operation(less='less')
        self.assertEqual(operation, QueryOperation.less)
        self.assertEqual(value, 'less')
    def test_tina_query__parse_operation_less_equal(self):
        operation, value = self.query._Query__parse_operation(less_equal='less_equal')
        self.assertEqual(operation, QueryOperation.less_equal)
        self.assertEqual(value, 'less_equal')
    def test_tina_query__parse_operation_greater(self):
        operation, value = self.query._Query__parse_operation(greater='greater')
        self.assertEqual(operation, QueryOperation.greater)
        self.assertEqual(value, 'greater')
    def test_tina_query__parse_operation_greater_equall(self):
        operation, value = self.query._Query__parse_operation(greater_equal='greater_equal')
        self.assertEqual(operation, QueryOperation.greater_equal)
        self.assertEqual(value, 'greater_equal')
    def test_tina_query__parse_operation_like(self):
        operation, value = self.query._Query__parse_operation(like='like')
        self.assertEqual(operation, QueryOperation.like)
        self.assertEqual(value, 'like')
    def test_tina_query__parse_operation_contains(self):
        operation, value = self.query._Query__parse_operation(contains='contains')
        self.assertEqual(operation, QueryOperation.contains)
        self.assertEqual(value, 'contains')
    def test_tina_query__parse_operation_exclude(self):
        operation, value = self.query._Query__parse_operation(exclude='exclude')
        self.assertEqual(operation, QueryOperation.exclude)
        self.assertEqual(value, 'exclude')
    def test_tina_query__parse_operation_raise(self):
        self.assertRaises(QuerySyntaxError, self.query._Query__parse_operation, good='xx')
