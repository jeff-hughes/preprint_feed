from elasticsearch_dsl import Date, DenseVector, Document, Keyword, Text

class Preprint(Document):
    """Stores data about preprints, including title, abstract, authors,
    dates, and document vectors."""
    source = Keyword(required=True)
    source_id = Keyword()  # article ID as identified by the source
    url = Keyword(required=True)

    publish_date = Date()
    modified_date = Date()
    stored_date = Date()

    title = Text()
    abstract = Text()
    authors = Text(multi=True)
        # this will probably need to change, but I'm not sure the best
        # way to store author names
    
    categories = Keyword(multi=True)
    keywords = Keyword(multi=True)

    doc_vector = DenseVector(dims=256)
        # may need to adjust number of dimensions

    class Index:
        name = "preprint"
        # can set settings here as well:
        # settings = { keys: values }


if __name__ == "__main__":
    import os
    from elasticsearch_dsl import connections

    connections.create_connection(hosts=[os.getenv("ELASTIC_HOST")], timeout=20)

    if not Preprint._index.exists():
        print("Initializing preprint index")
        Preprint.init()