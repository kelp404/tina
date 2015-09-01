from datetime import datetime
from .exceptions import BadValueError


class Property(object):
    def __init__(self, default=None, required=False, analyzer=None, mapping=None):
        """
        Init the Property.
        :param default: The default value.
        :param required: {bool} Is this failed required?
        :param analyzer: {str} The analyzer of this member for elasticsearch mapping.
        :param mapping: {dic} The mapping of sub-members.
            'email': {
                'type': 'string',
                'analyzer': 'email_url',
            },
        :return:
        """
        self.document_class = None
        self.name = None
        self.default = default
        self.required = required
        self.analyzer = analyzer
        self.mapping = mapping

    def __get__(self, document_instance, document_class):
        if document_instance is None:
            return self

        value = document_instance._document.get(self.name)
        if value is None:
            return None
        return self._to_python(value)

    def __set__(self, document_instance, value):
        if value is None:
            if self.required:
                raise BadValueError('%s is required' % self.name)
            document_instance._document[self.name] = None
        else:
            document_instance._document[self.name] = self._to_json(value)

    def __property_config__(self, document_class, property_name):
        """
        Setup the property.
        :param document_class: {Document} The document class.
        :param property_name: {string} The property name in the document.
        :return:
        """
        self.document_class = document_class
        if self.name is None:
            self.name = property_name

    def _to_python(self, value):
        """
        Convert the value to Python format.
        :param value:
        :return:
        """
        return str(value)
    def _to_json(self, value):
        """
        Convert the value to ElasticSearch format.
        :param value:
        :return:
        """
        return str(value)

class StringProperty(Property):
    _to_python = str
    _to_json = str

class IntegerProperty(Property):
    _to_python = int
    _to_json = int

class FloatProperty(Property):
    _to_python = float
    _to_json = float

class BooleanProperty(Property):
    _to_python = bool
    _to_json = bool

class DateTimeProperty(Property):
    def __init__(self, auto_now=False, *args, **kwargs):
        super(DateTimeProperty, self).__init__(*args, **kwargs)
        self.auto_now = auto_now

    @classmethod
    def _to_python(cls, value):
        """
        Convert value to python format.
        :param value: {datetime}, {string}
        :return: {datetime}
        """
        if isinstance(value, str):
            try:
                value = value.split('.', 1)[0] # strip out microseconds
                value = value[0:19] # remove timezone
                value = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
            except ValueError as e:
                raise ValueError('Invalid ISO date/time %r [%s]' % (value, str(e)))
        return value
    @classmethod
    def _to_json(cls, value):
        """
        Convert value to json format.
        :param value: {datetime}, {string}
        :return: {string}
        """
        if isinstance(value, str):
            return value
        return value.replace(microsecond=0).isoformat() + 'Z'

class ListProperty(Property):
    def __init__(self, item_type=None, *args, **kwargs):
        """
        Init list property.
        You should handle items details, like the type of items and datetime format...
        :param item_type: {type} The item type of the list.
                    tina will use this tye for creating instance form database.
        :param args:
        :param kwargs:
        :return:
        """
        super(ListProperty, self).__init__(*args, **kwargs)
        self.item_type = item_type

    def __get__(self, document_instance, document_class):
        if document_instance is None:
            return self
        if document_instance._document.get(self.name) is None:
            return None
        return document_instance._document.get(self.name)

    def __set__(self, document_instance, value):
        if value is None:
            if self.required:
                raise BadValueError('%s is required' % self.name)
            document_instance._document[self.name] = None
            return

        if not isinstance(value, list):
            raise BadValueError('%s should be list' % self.name)
        if self.item_type:
            document_instance._document[self.name] = [None if x is None else self.item_type(x) for x in value]
        else:
            document_instance._document[self.name] = value

class DictProperty(Property):
    """
    DictProperty is very simple property. It doesn't check every things.
    Like data type, convert datetime format and more... you should handle that by yourself.
    """
    def __init__(self, *args, **kwargs):
        """
        Init dict property.
        :param args:
        :param kwargs:
        :return:
        """
        super(DictProperty, self).__init__(*args, **kwargs)

    def __get__(self, document_instance, document_class):
        if document_instance is None:
            return self
        return document_instance._document[self.name]

    def __set__(self, document_instance, value):
        document_instance._document[self.name] = value

class ReferenceProperty(Property):
    def __init__(self, reference_class, *args, **kwargs):
        from .document import Document

        super(ReferenceProperty, self).__init__(*args, **kwargs)
        if not issubclass(reference_class, Document):
            raise TypeError('Reference class should be Document')
        self.reference_class = reference_class

    def __get__(self, document_instance, document_class):
        if document_instance is None:
            return self
        return document_instance._reference_document.get(self.name) or document_instance._document.get(self.name)

    def __set__(self, document_instance, value):
        if value is None:
            if self.required:
                raise BadValueError('%s is required' % self.name)
            document_instance._document[self.name] = None
            document_instance._reference_document[self.name] = None
        elif isinstance(value, str):
            # set reference id
            document_instance._document[self.name] = value
        else:
            if not isinstance(value, self.reference_class):
                raise ValueError('Value should be %s' % self.document_class)
            document_instance._document[self.name] = value._id
            document_instance._reference_document[self.name] = value
