import time
from datetime import datetime
from . import utils
from .query import Query
from .properties import Property, BooleanProperty, IntegerProperty, FloatProperty,\
    DateTimeProperty, StringProperty, ReferenceProperty, ListProperty
from .exceptions import NotFoundError, TransportError
from .deep_query import update_reference_properties


class Document(object):
    """
    :attribute _index: {string} You can set index name by this attribute.
    :attribute _settings: {dict} You can set index settings by this attribute.
    :attribute _id: {string}
    :attribute _version: {int}
    :attribute _document: {dict} {'property_name': (value)}
    :attribute _reference_document: {dict} {'property_name': {Document}}
    :attribute _properties: {dict} {'property_name': {Property}}
    :attribute _es: {Elasticsearch}
    :attribute _index_name: {string}
    """
    _id = StringProperty()
    _version = IntegerProperty()
    _es = utils.get_elasticsearch()

    def __new__(cls, *args, **kwargs):
        cls._properties = cls.__get_properties()
        cls._properties_in = cls  # memo cls._properties from which class
        return object.__new__(cls, *args)

    def __init__(self, **kwargs):
        super(Document, self).__init__()
        self._document = {}
        self._reference_document = {}
        for property_name, property in self._properties.items():
            if property_name in kwargs.keys():
                setattr(self, property_name, kwargs[property_name])
            else:
                setattr(self, property_name, property.default)

    @classmethod
    def __get_properties(cls):
        """
        Get properties of this class.
        :return: {dict} {'property_name': {Property}}
        """
        properties = {}
        for attribute_name in dir(cls):
            if attribute_name.startswith('__'):
                continue
            attribute = getattr(cls, attribute_name)
            if isinstance(attribute, Property):
                properties[attribute_name] = attribute
                attribute.__property_config__(cls, attribute_name)
        return properties

    @classmethod
    def get_properties(cls):
        """
        Some time the class didn't call initial function but need get properties list.
        :return: {dict} {'property_name': {Property}}
        """
        if not hasattr(cls, '_properties_in') or not cls is cls._properties_in\
                or not hasattr(cls, '_properties'):
            cls._properties = cls.__get_properties()
            cls._properties_in = cls
        return cls._properties

    @classmethod
    def get_index_name(cls):
        if not hasattr(cls, '_index_name') or not cls._index_name:
            if hasattr(cls, '_index') and cls._index:
                cls._index_name = '%s%s' % (utils.get_index_prefix(), cls._index)
            else:
                cls._index_name = '%s%s' % (utils.get_index_prefix(), cls.__name__.lower())
        return cls._index_name

    @classmethod
    def get_index_settings(cls):
        if not hasattr(cls, '_settings') or not cls._settings:
            return None
        return cls._settings

    @classmethod
    def get(cls, ids, fetch_reference=True):
        """
        Get documents by ids.
        :param ids: {list or string} The documents' id.
        :return: {list or Document}
        """
        if ids is None or ids == '':
            return None
        if isinstance(ids, list) and not len(ids):
            return []
        es = utils.get_elasticsearch()
        if isinstance(ids, list):
            # fetch documents
            if not len(ids):
                return []

            response = es.mget(
                index=cls.get_index_name(),
                doc_type=cls.__name__,
                body={
                    'ids': list([x for x in set(ids) if x])
                },
            )
            result_table = {x['_id']: x for x in response['docs'] if x['found']}
            result = []
            for document_id in ids:
                document = result_table.get(document_id)
                if document:
                    result.append(cls(_id=document['_id'], _version=document['_version'], **document['_source']))
            if fetch_reference:
                update_reference_properties(result)
            return result

        # fetch the document
        try:
            response = es.get(
                index=cls.get_index_name(),
                doc_type=cls.__name__,
                id=ids,
            )
            result = cls(_id=response['_id'], _version=response['_version'], **response['_source'])
            if fetch_reference:
                update_reference_properties([result])
            return result
        except NotFoundError:
            return None

    @classmethod
    def exists(cls, id):
        es = utils.get_elasticsearch()
        return es.exists(
            index=cls.get_index_name(),
            doc_type=cls.__name__,
            id=id,
        )

    @classmethod
    def where(cls, *args, **kwargs):
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
            unlike,
            contains,
        ]
        :return: {tina.query.Query}
        """
        query = Query(cls)
        return query.intersect(*args, **kwargs)

    @classmethod
    def all(cls):
        """
        The query for all documents.
        :return: {tina.query.Query}
        """
        return Query(cls)

    @classmethod
    def refresh(cls):
        """
        Explicitly refresh the index, making all operations performed
        since the last refresh available for search.
        `<http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-refresh.html>`_
        """
        cls._es.indices.refresh(index=cls.get_index_name())

    @classmethod
    def update_mapping(cls):
        """
        Update the index mapping.
        https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
        https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html
        """
        try:
            cls._es.indices.create(index=cls.get_index_name())
            time.sleep(1)
        except TransportError as e:
            if e.status_code != 400:
                raise e

        # close index
        cls._es.indices.close(index=cls.get_index_name())

        # put settings
        if cls.get_index_settings():
            cls._es.indices.put_settings({
                'settings': {
                    'index': cls.get_index_settings(),
                },
            }, index=cls.get_index_name())

        # put mapping
        mapping = {}
        for name, property in cls.get_properties().items():
            if name in ['_id', '_version']:
                continue
            if property.mapping:
                mapping[name] = {'properties': property.mapping}
                continue

            field = {}
            if isinstance(property, StringProperty):
                field['type'] = 'string'
            elif isinstance(property, BooleanProperty):
                field['type'] = 'boolean'
            elif isinstance(property, IntegerProperty):
                field['type'] = 'long'
            elif isinstance(property, FloatProperty):
                field['type'] = 'double'
            elif isinstance(property, DateTimeProperty):
                field['type'] = 'date'
                field['format'] = 'dateOptionalTime'
            elif isinstance(property, ReferenceProperty):
                field['type'] = 'string'
                field['analyzer'] = 'keyword'
            elif isinstance(property, ListProperty):
                if property.item_type is str:
                    field['type'] = 'string'
                elif property.item_type is bool:
                    field['type'] = 'boolean'
                elif property.item_type is int:
                    field['type'] = 'long'
                elif property.item_type is float:
                    field['type'] = 'double'
                elif property.item_type is datetime:
                    field['type'] = 'date'
                    field['format'] = 'dateOptionalTime'
            if property.analyzer:
                field['analyzer'] = property.analyzer

            if field:
                mapping[name] = field
        cls._es.indices.put_mapping(
            cls.__name__,
            {
                'properties': mapping,
            },
            index=cls.get_index_name()
        )

        # open index
        cls._es.indices.open(index=cls.get_index_name())

    def save(self, synchronized=False):
        """
        Save the document.
        """
        if self._version is None:
            self._version = 0
        for property_name, property in self._properties.items():
            if isinstance(property, DateTimeProperty) and property.auto_now and not getattr(self, property_name):
                setattr(self, property_name, datetime.utcnow())
        document = self._document.copy()
        del document['_id']
        del document['_version']
        result = self._es.index(
            index=self.get_index_name(),
            doc_type=self.__class__.__name__,
            id=self._id,
            version=self._version,
            body=document
        )
        self._id = result.get('_id')
        self._version = result.get('_version')
        if synchronized:
            self._es.indices.refresh(index=self.get_index_name())
        return self

    def delete(self, synchronized=False):
        """
        Delete the document.
        """
        if not self._id:
            return None

        self._es.delete(
            index=self.get_index_name(),
            doc_type=self.__class__.__name__,
            id=self._id,
        )
        if synchronized:
            self._es.indices.refresh(index=self.get_index_name())
        return self
