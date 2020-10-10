"""Test script for figuring out how to query from elasticsearch"""

import os
from itertools import islice

from elasticsearch_dsl import connections
from elastic.elastic_mapping import Preprint

elastic_host = f"{os.getenv('ELASTIC_ADMIN_USER')}:{os.getenv('ELASTIC_ADMIN_PASS')}@{os.getenv('ELASTIC_HOST')}"
connections.create_connection(hosts=[elastic_host], timeout=20)

# making a typical search query ------------------------------------------

search = Preprint.search() \
    .query("match_phrase", title="reinforcement learning")

response = search.execute()

print(response.success())
print(response.took)
print(response.hits.total)

print(response.hits.hits[0].to_dict())

# not sure if this is the best way to do this, but...it seems to work
test = Preprint(meta=response.hits.hits[0].to_dict(), **response.hits.hits[0].to_dict()['_source'])
print(test)
print(test.title)
print(test.url)
print()


# scrolling through batches of results -----------------------------------

# helper function because the `search.scan()` method pulls results in
# batches but produces an iterator that returns them one at a time; which
# is fine unless you want to train in batches!
def chunk(iterable, size):
    """Given an iterator, this yields tuples of size `size` until
    iterator is exhausted.
    """
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, size))
        if not chunk:
            return
        yield chunk


batch_size = 10
search = Preprint.search()
search._params.update(scroll="1m", size=batch_size)
results = chunk(search.scan(), batch_size)

for _ in range(3):
    next_batch = next(results)
    titles = [p.title for p in next_batch]
    print(titles)
print()


# can also use elasticsearch's lower-level interface, which gives access
# to whole search result, including number of hits, etc.

import elasticsearch

es = elasticsearch.Elasticsearch(hosts=[elastic_host], timeout=20)

batch_size = 5
search = es.search(index="preprint", params={"scroll": "1m", "size": batch_size})
print(search)

# iterate with this
search2 = es.scroll(scroll_id=search["_scroll_id"], params={"scroll": "1m"})
print(search2)