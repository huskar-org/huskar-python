from __future__ import absolute_import

import pytest

from huskar_sdk_v2.six import unicode
from huskar_sdk_v2.utils import Counter, join_url


def test_counter():
    c = Counter(1)
    assert c.get() == 1
    assert unicode(c) == '<Counter init=1 now=1>'

    c.incr()
    assert c.get() == 2
    assert unicode(c) == '<Counter init=1 now=2>'

    c.reset()
    assert c.get() == 1
    assert unicode(c) == '<Counter init=1 now=1>'


@pytest.mark.parametrize('input,output', [
    (['http://example.com:8080', '/api', 'test', '233'],
     'http://example.com:8080/api/test/233'),
    (['http://example.com:8080', '/api/', 'test/', '233'],
     'http://example.com:8080/api/test/233'),
    (['http://example.com:8080', '/api/', '/test/', '233'],
     'http://example.com:8080/api/test/233'),
    (['http://example.com:8080', '/api', '/test/', '/233'],
     'http://example.com:8080/api/test/233'),
    (['http://example.com', '/api/', 'test/', '233'],
     'http://example.com/api/test/233'),
    (['http://example.com', '/api/', 'test/', '233/'],
     'http://example.com/api/test/233/'),
    (['http://example.com', '/api/test/', '233'],
     'http://example.com/api/test/233'),
    (['http://example.com', '/api/test/', '/233/'],
     'http://example.com/api/test/233/'),
    (['example.com', '/api/test/', '/233/'],
     'http://example.com/api/test/233/'),
    (['example.com:8080', '/api/test/', '/233/'],
     'http://example.com:8080/api/test/233/'),
    (['example.com:8080', '/api/test/'],
     'http://example.com:8080/api/test/'),
    (['example.com:8080', '/api/test'],
     'http://example.com:8080/api/test'),
    (['example.com:8080', 'api/test/'],
     'http://example.com:8080/api/test/'),
    (['example.com:8080', 'api/test'],
     'http://example.com:8080/api/test'),
])
def test_join_url(input, output):
    assert join_url(*input) == output
