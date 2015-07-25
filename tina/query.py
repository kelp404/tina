import re
from datetime import datetime
from .deep_query import update_reference_properties
from .exceptions import NotFoundError, PropertyNotExist, QuerySyntaxError


class QueryOperation(object):
    normal_operation_mask = 0x3F
    unequal = 0x000
    equal = 0x001
    less = 0x002
    less_equal = 0x003
    greater = 0x004
    greater_equal = 0x005
    like = 0x011  # only for string
    unlike = 0x010  # only for string
    contains = 0x021  # it is mean `in`
    exclude = 0x020

    intersection = 0x040
    union = 0x080

    all = 0x0100

    # asc: 0x200, desc 0x000
    # order: 0x400
    order_asc = 0x600
    order_desc = 0x400


class QueryCell(object):
    def __init__(self, operation, member=None, value=None, sub_queries=None):
        self.member = member
        self.operation = operation
        self.value = value
        self.sub_queries = sub_queries


class Query(object):
    """
    A tina query object.
    """
    def __init__(self, document_class):
        self.contains_empty = False
        self.document_class = document_class
        self.items = [
            QueryCell(QueryOperation.all)
        ]


    # -----------------------------------------------------
    # The methods for appending the query.
    # -----------------------------------------------------
    def where(self, *args, **kwargs):
        """
        It is intersect.
        """
        return self.intersect(*args, **kwargs)
    def intersect(self, *args, **kwargs):
        """
        Intersect the query.
        :param args:
            The member's name of the document or
                the sub queries' lambda function.
        :param kwargs: [
            unequal,
            equal,
            less,
            less_equal,
            greater,
            greater_equal,
            like,
            contains,
        ]
        :return: {tina.query.Query}
        """
        if isinstance(args[0], str):
            # .and('member', equal='')
            member = args[0]
            if member.split('.', 1)[0] not in self.document_class.get_properties().keys():
                raise PropertyNotExist('%s not in %s' % (member, self.document_class.__name__))
            operation_code, value = self.__parse_operation(**kwargs)
            if self.contains_empty or (operation_code & QueryOperation.contains == QueryOperation.contains and not value):
                self.contains_empty = True
                return self
            self.items.append(QueryCell(
                QueryOperation.intersection | operation_code,
                member=member,
                value=value,
            ))
        else:
            # .and(lambda x: x.where())
            sub_query = args[0](self.document_class)
            if self.contains_empty or sub_query.contains_empty:
                self.contains_empty = True
                return self
            queries = sub_query.items
            self.items.append(QueryCell(
                QueryOperation.intersection,
                sub_queries=queries
            ))
        return self

    def union(self, *args, **kwargs):
        """
        Union the query.
        :param args:
            The member's name of the document or
                the sub queries' lambda function.
        :param kwargs: [
            unequal,
            equal,
            less,
            less_equal,
            greater,
            greater_equal,
            like,
            unlike,
            contains,
        ]
        :return: {tina.query.Query}
        """
        if isinstance(args[0], str):
            # .or('member', equal='')
            member = args[0]
            if member.split('.', 1)[0] not in self.document_class.get_properties().keys():
                raise PropertyNotExist('%s not in %s' % (member, self.document_class.__name__))
            operation_code, value = self.__parse_operation(**kwargs)
            self.items.append(QueryCell(
                QueryOperation.union | operation_code,
                member=member,
                value=value,
            ))
        else:
            # .or(lambda x: x.where())
            func = args[0]
            queries = func(self.document_class).items
            self.items.append(QueryCell(
                QueryOperation.union,
                sub_queries=queries
            ))
        return self

    def order_by(self, member, descending=False):
        """
        Append the order query.
        :param member: {string} The property name of the document.
        :param descending: {bool} Is sorted by descending?
        :return: {tina.query.Query}
        """
        if member.split('.', 1)[0] not in self.document_class.get_properties().keys():
            raise PropertyNotExist('%s not in %s' % (member, self.document_class.__name__))
        if descending:
            operation_code = QueryOperation.order_desc
        else:
            operation_code = QueryOperation.order_asc
        self.items.append(QueryCell(
            operation_code,
            member=member
        ))
        return self


    # -----------------------------------------------------
    # The methods for fetch documents by the query.
    # -----------------------------------------------------
    def fetch(self, limit=1000, skip=0, fetch_reference=True):
        """
        Fetch documents by the query.
        :param limit: {int} The size of the pagination. (The limit of the result items.)
        :param skip: {int} The offset of the pagination. (Skip x items.)
        :returns: {tuple}
            ({list}[{Document}], {int}total)
            The documents.
            The total items.
        """
        if self.contains_empty:
            return [], 0

        es = self.document_class._es
        def __search():
            return es.search(
                index=self.document_class.get_index_name(),
                body=self.__generate_elasticsearch_search_body(self.items, limit, skip),
                version=True
            )
        try:
            search_result = __search()
        except NotFoundError as e:
            if 'IndexMissingException' in str(e):  # try to create index
                es.indices.create(index=self.document_class.get_index_name())
                search_result = __search()
            else:
                raise e

        result = []
        for hits in search_result['hits']['hits']:
            result.append(self.document_class(_id=hits['_id'], _version=hits['_version'], **hits['_source']))
        if fetch_reference:
            update_reference_properties(result)
        return result, search_result['hits']['total']

    def has_any(self):
        if self.contains_empty:
            return False

        es = self.document_class._es
        query = self.__compile_queries(self.items)[0]
        if query is None:
            query = {'match_all': {}}
        return es.search_exists(
            index=self.document_class.get_index_name(),
            body={'query': query},
        )

    def first(self, fetch_reference=True):
        """
        Fetch the first document.
        :return: {tina.document.Document or None}
        """
        documents, total = self.fetch(1, 0, fetch_reference=fetch_reference)
        if total == 0:
            return None
        else:
            return documents[0]

    def count(self):
        """
        Count documents by the query.
        :return: {int}
        """
        if self.contains_empty:
            return 0

        query, _ = self.__compile_queries(self.items)
        es = self.document_class._es
        if query is None:
            def __count():
                return es.count(self.document_class.get_index_name())
            try:
                count_result = __count()
            except NotFoundError as e:
                if 'IndexMissingException' in str(e):  # try to create index
                    es.indices.create(index=self.document_class.get_index_name())
                    count_result = __count()
                else:
                    raise e
        else:
            def __count():
                return es.count(
                    index=self.document_class.get_index_name(),
                    body={
                        'query': query
                    },
                )
            try:
                count_result = __count()
            except NotFoundError as e:
                if 'IndexMissingException' in str(e):  # try to create index
                    es.indices.create(index=self.document_class.get_index_name())
                    count_result = __count()
                else:
                    raise e
        return count_result['count']

    def group_by(self, member, limit=10, descending=True):
        """
        Aggregations
        http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations.html
        :param member: {string} The property name of the document.
        :param limit: {int} The number of returns.
        :param descending: {bool} Is sorted by descending?
        :returns: {list}
            {list}[{dict}]
            {
                doc_count: {int},
                key: 'term'
            }
        """
        if member.split('.', 1)[0] not in self.document_class.get_properties().keys():
            raise PropertyNotExist('%s not in %s' % (member, self.document_class.__name__))
        es = self.document_class._es
        es_query, sort_items = self.__compile_queries(self.items)
        query_body = {
            'size': 0,
            'aggs': {
                'group': {
                    'terms': {
                        'field': member,
                        'size': limit,
                        'order': {
                            '_count': 'desc' if descending else 'asc'
                        }
                    }
                }
            }
        }
        if es_query:
            query_body['query'] = es_query

        def __search():
            return es.search(
                index=self.document_class.get_index_name(),
                body=query_body,
            )
        try:
            search_result = __search()
        except NotFoundError as e:
            if 'IndexMissingException' in str(e):  # try to create index
                es.indices.create(index=self.document_class.get_index_name())
                search_result = __search()
            else:
                raise e

        return search_result['aggregations']['group']['buckets']


    # -----------------------------------------------------
    # Private methods.
    # -----------------------------------------------------
    def __generate_elasticsearch_search_body(self, queries, limit=None, skip=None):
        """
        Generate the elastic search search body.
        :param queries: {list} The tina query items.
        :param limit: {int} The limit of the result items.
        :param skip: {int} Skip x items.
        :return: {dict} The elastic search search body
        """
        es_query, sort_items = self.__compile_queries(queries)
        result = {
            'from': skip,
            'size': limit,
            'fields': ['_source'],
            'sort': sort_items,
        }
        if es_query:
            result['query'] = es_query
        return result

    def __compile_queries(self, queries):
        """
        Compile tina query cells to the elastic search query.
        :param queries: {list} The tina query cells.
        :returns: {tuple} ({dict or None}, {list})
            The elastic search query dict.
            The elastic search sort list.
        """
        sort_items = []
        necessary_items = []
        optional_items = []
        last_item_is_necessary = False
        for query in queries:
            if query.sub_queries:
                # compile sub queries
                sub_query, sub_sort_items = self.__compile_queries(query.sub_queries)
                if sub_query and query.operation & QueryOperation.intersection == QueryOperation.intersection:
                    # intersect
                    necessary_items.append(sub_query)
                    last_item_is_necessary = True
            else:
                if isinstance(query.value, str):
                    query.value = re.sub(r'[<>]', '', query.value)
                if query.operation & QueryOperation.intersection == QueryOperation.intersection:
                    # intersect
                    query_item = self.__compile_query(query)
                    if query_item:
                        necessary_items.append(query_item)
                        last_item_is_necessary = True
                elif query.operation & QueryOperation.union == QueryOperation.union:
                    # union
                    query_item = self.__compile_query(query)
                    if query_item:
                        if last_item_is_necessary:
                            necessary_item = necessary_items.pop()
                            optional_items.append(necessary_item)
                        optional_items.append(query_item)
                        last_item_is_necessary = False
                elif query.operation & QueryOperation.order_asc == QueryOperation.order_asc:
                    # order asc
                    sort_items.append({
                        query.member: {
                            'order': 'asc',
                            'ignore_unmapped': True,
                            'missing': '_first',
                        }
                    })
                elif query.operation & QueryOperation.order_desc == QueryOperation.order_desc:
                    # order desc
                    sort_items.append({
                        query.member: {
                            'order': 'desc',
                            'ignore_unmapped': True,
                            'missing': '_last',
                        }
                    })

        if len(necessary_items):
            optional_items.append({
                'bool': {
                    'should': necessary_items,
                    'minimum_should_match': len(necessary_items),
                }
            })
        if len(optional_items):
            query = {
                'bool': {
                    'should': optional_items,
                    'minimum_should_match': 1,
                }
            }
        else:
            query = None
        return query, sort_items
    def __compile_query(self, query):
        """
        Parse the tina query cell to elastic search query.
        :param query: The tina query cell.
        :return: {dict} The elastic search query.
        """
        operation = query.operation & QueryOperation.normal_operation_mask
        if operation & QueryOperation.like == QueryOperation.like:
            return {
                'bool': {
                    'should': [
                        {
                            'match': {
                                query.member: {
                                    'query': query.value,
                                    'operator': 'and',
                                }
                            }
                        },
                        {
                            'regexp': {
                                query.member: '.*%s.*' % query.value
                            }
                        },
                    ]
                }
            }
        elif operation & QueryOperation.unlike == QueryOperation.unlike:
            return {
                'bool': {
                    'minimum_should_match': 2,
                    'should': [
                        {
                            'bool': {
                                'must_not': {
                                    'match': {
                                        query.member: {
                                            'query': query.value,
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
                                        query.member: '.*%s.*' % query.value
                                    }
                                }
                            }
                        },
                    ]
                }
            }
        elif operation & QueryOperation.contains == QueryOperation.contains:
            return {
                'bool': {
                    'should': [{'match': {query.member: {'query': x, 'operator': 'and'}}} for x in query.value],
                }
            }
        elif operation & QueryOperation.exclude == QueryOperation.exclude:
            return {
                'bool': {
                    'minimum_should_match': len(query.value),
                    'should': [{'bool': {'must_not': {'match': {query.member: {'query': x, 'operator': 'and'}}}}} for x in query.value],
                }
            }
        elif operation & QueryOperation.greater_equal == QueryOperation.greater_equal:
            if isinstance(query.value, datetime):
                query_value = self.__convert_datetime_for_query(query.value)
            else:
                query_value = query.value
            return {
                'range': {
                    query.member: {
                        'gte': query_value
                    }
                }
            }
        elif operation & QueryOperation.greater == QueryOperation.greater:
            if isinstance(query.value, datetime):
                query_value = self.__convert_datetime_for_query(query.value)
            else:
                query_value = query.value
            return {
                'range': {
                    query.member: {
                        'gt': query_value
                    }
                }
            }
        elif operation & QueryOperation.less_equal == QueryOperation.less_equal:
            if isinstance(query.value, datetime):
                query_value = self.__convert_datetime_for_query(query.value)
            else:
                query_value = query.value
            return {
                'range': {
                    query.member: {
                        'lte': query_value
                    }
                }
            }
        elif operation & QueryOperation.less == QueryOperation.less:
            if isinstance(query.value, datetime):
                query_value = self.__convert_datetime_for_query(query.value)
            else:
                query_value = query.value
            return {
                'range': {
                    query.member: {
                        'lt': query_value
                    }
                }
            }
        elif operation & QueryOperation.equal == QueryOperation.equal:
            if query.value is None:
                return {
                    'filtered': {
                        'filter': {
                            'missing': {
                                'field': query.member
                            }
                        }
                    }
                }
            else:
                return {
                    'match': {
                        query.member: {
                            'query': query.value,
                            'operator': 'and',
                        }
                    }
                }
        elif operation & QueryOperation.unequal == QueryOperation.unequal:
            if query.value is None:
                return {
                    'bool': {
                        'must_not': {
                            'filtered': {
                                'filter': {
                                    'missing': {
                                        'field': query.member
                                    }
                                }
                            }
                        }
                    }
                }
            else:
                return {
                    'bool': {
                        'must_not': {
                            'match': {
                                query.member: {
                                    'query': query.value,
                                    'operator': 'and',
                                }
                            }
                        }
                    }
                }

    def __convert_datetime_for_query(self, date_time):
        """
        Convert datetime data for query.
        :param date_time: {datetime}
        :return: {string} "yyyy-MM-ddTHH:mm:ss
        """
        return date_time.strftime('%Y-%m-%dT%H:%M:%S')

    def __parse_operation(self, **kwargs):
        """
        Parse the operation and value of the query **kwargs.
        :returns: {QueryOperation}, {object}
            QueryOperation: The query operation code.
            object: The query operation value.
        """
        if len(kwargs) != 1:
            raise QuerySyntaxError
        key = list(kwargs.keys())[0]
        try:
            operation = {
                'equal': QueryOperation.equal,
                'unequal': QueryOperation.unequal,
                'less': QueryOperation.less,
                'less_equal': QueryOperation.less_equal,
                'greater': QueryOperation.greater,
                'greater_equal': QueryOperation.greater_equal,
                'like': QueryOperation.like,
                'unlike': QueryOperation.unlike,
                'contains': QueryOperation.contains,
                'exclude': QueryOperation.exclude,
            }[key]
        except KeyError:
            raise QuerySyntaxError
        return operation, kwargs[key]
