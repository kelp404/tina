#tina ![circle-ci](https://circleci.com/gh/kelp404/tina.png?circle-token=76f080d70a1b9fdd6e01ff5f55b0acebbf35f5cd)

An elasticsearch client on Python 3.4.

![tina](_tina.gif)


##Installation
>```bash
$ git submodule add https://github.com/kelp404/tina.git
$ git submodule add https://github.com/kelp404/elasticsearch-py.git
$ pip3 install urllib3
$ pip3 install certifi
$ pip3 install ujson
```



##Django settings
>
```python
# settings.py
TINA_ELASTICSEARCH_URL = 'https://username:password@domain.com:9200'
TINA_INDEX_PREFIX = 'prefix_'  # The prefix of the index name.
```



##Document
>
```python
# example:
from tina import db
from .models import Account
# define your data model
class SampleModel(db.Document):
    _index = 'samples'  # You can set index name by this attribute.
    _settings = {  # You can set index settings by this attribute.
        'analysis': {
            'analyzer': {
                'email_url': {
                    'type': 'custom',
                    'tokenizer': 'uax_url_email',
                }
            }
        }
    }
    name = db.StringProperty()
    email = db.StringProperty(required=True, analyzer='email_url')
    is_vip = db.BooleanProperty(default=False)
    quota = db.FloatProperty(default=0.0)
    account = db.ReferenceProperty(Account)
    items = db.ListProperty(default=[], mapping={'value': {'type': 'string'}})
    created_at = db.DateTimeProperty(auto_now=True)
```

**Properties**
>```python
_id: {string}
_version: {int}
```

**Methods**
>```python
def get(cls, ids, rev=None, db=None, dynamic_properties=True):
    """
    Get documents by ids.
    :param ids: {list or string} The documents' id.
    :return: {list or Document}
    """
# example:
#    Get the document by the id.
#    The result document is SampleModel's instance.
    document = SampleModel.get('byMQ-ULRSJ291RG_eEwSfQ')
#    Get the documents by ids.
#    The result documents is the list. There are SampleModels' instance in the list.
    documents = SampleModel.get([
        'byMQ-ULRSJ291RG_eEwSfQ',
        'byMQ-ULRSJ291RG_eEwSfc',
    ])
```
```python
def exists(cls, id):
    """
    Is the document exists?
    :param id: {string} The documents' id.
    :return: {bool}
    """
    is_exist = SampleModel.exists('byMQ-ULRSJ291RG_eEwSfQ')
```
```python
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
        exclude,
    ]
    :return: {tina.query.Query}
    """
```
```python
def all(cls):
    """
    The query for all documents.
    :return: {tina.query.Query}
    """
```
```python
def refresh(cls):
    """
    Explicitly refresh the index, making all operations performed
    """
```
```python
def update_mapping(cls):
    """
    Update the index mapping.
    """
```
```python
def save(self, synchronized=False):
    """
    Save the document.
    """
```
```python
def delete(self, synchronized=False):
    """
    Delete the document.
    """
```



##Query
>The tina query.

**Methods**
>```python
def where(self, *args, **kwargs):
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
        exclude,
    ]
    :return: {tina.query.Query}
    """
```
```python
def order_by(self, member, descending=False):
    """
    Append the order query.
    :param member: {string} The property name of the document.
    :param descending: {bool} Is sorted by descending.
    :return: {tina.query.Query}
    """
```
```python
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
```
```python
def has_any(self):
    """
    Are there any documents match with the query?
    :return: {bool}
    """
```
```python
def first(self, fetch_reference=True):
    """
    Fetch the first document.
    :return: {tina.document.Document or None}
    """
```
```python
def count(self):
    """
    Count documents by the query.
    :return: {int}
    """
```
```python
def sum(self, member):
    """
    Sum the field of documents by the query.
    :param member: {string} The property name of the document.
    :return: {int}
    """
```
```python
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
```



##Examples
>```sql
select * from "ExampleModel" where "name" = "tina"
```
```python
models, total = ExampleModel.where('name', equal='tina').fetch()
```

---
>```sql
select * from "ExampleModel" where "name" = "tina" and "email" = "kelp@phate.org"
```
```python
models, total = ExampleModel.where('name', equal='tina')\
        .where('email', equal='kelp@phate.org')\
        .fetch()
```

---
>```sql
select * from "ExampleModel" where "name" like "%tina%" or "email" like "%tina%"
```
```python
models, total = ExampleModel.where(lambda x:
    x.where('name', like='tina')
    .union('email', like='tina')
).fetch()
```

---
>```sql
select * from "ExampleModel" where "category" = 1 or "category" = 3
        order by "created_at" limit 20 offset 20
```
```python
models, total = ExampleModel.where('category', contains=[1, 3])\
        .order_by('created_at').fetch(20, 20)
```

---
>Fetch the first item.
```sql
select * from "ExampleModel" where "age" >= 10
         order by "created_at" desc limit 1
```
```python
model = ExampleModel.where('age', greater_equal=10)\
        .order_by('created_at', descending=True).first()
```

---
>Count items.
```sql
select count(*) from "ExampleModel" where "age" < 10
```
```python
count = ExampleModel.where('age', less=10).count()
```



##Properties
>https://github.com/kelp404/tina/blob/master/tina/properties.py
+ Property
+ StringProperty
+ IntegerProperty
+ BooleanProperty
+ FloatProperty
+ DateTimeProperty
+ ListProperty
+ DictProperty
+ ReferenceProperty


##Requirement
>```bash
$ git submodule update --init
$ pip3 install -r pip_requirements.txt
```



##unit-test
>```bash
$ python3 test.py
```



##Note
>The default tokenizer is case-insensitive. If we set the `tokenizer` as `keyword`, it will be case-sensitive.
If we want the field to be case-insensitive with `keyword`, we need to set the `filter` as `lowercase`.

-

>There are issues about ElasticSearch.  
If your OS X is 10.9.3, your default Java is 1.6. ElasticSearch 1.2.0 required Java 1.7.
Run ElasticSearch 1.2.0 on Java 1.6 will pop the message like this:
```
 Exception in thread "main" java.lang.UnsupportedClassVersionError: org/elasticsearch/bootstrap/Elasticsearch : Unsupported major.minor version 51.0
 at java.lang.ClassLoader.defineClass1(Native Method)
 at java.lang.ClassLoader.defineClassCond(ClassLoader.java:631)
 at java.lang.ClassLoader.defineClass(ClassLoader.java:615)
 at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:141)
 at java.net.URLClassLoader.defineClass(URLClassLoader.java:283)
 at java.net.URLClassLoader.access$000(URLClassLoader.java:58)
 at java.net.URLClassLoader$1.run(URLClassLoader.java:197)
 at java.security.AccessController.doPrivileged(Native Method)
 at java.net.URLClassLoader.findClass(URLClassLoader.java:190)
 at java.lang.ClassLoader.loadClass(ClassLoader.java:306)
 at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:301)
 at java.lang.ClassLoader.loadClass(ClassLoader.java:247)
Could not find the main class: org.elasticsearch.bootstrap.Elasticsearch.  Program will exit.
```



##References
>+ [elasticsearch-queries](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-queries.html)
