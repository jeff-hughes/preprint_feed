"""Loads all historical OSF Preprints data into database

This script only needs to be run once when the database is being set up;
it reads in historical data from a set of pickled python files
(downloaded from the API in advance) and feeds them into the database.
Database must already be running and the document schema/mapping already
created.
"""

import os
import glob
import pickle
from datetime import datetime

from elasticsearch_dsl import connections
from elasticsearch.helpers import bulk

from elastic.elastic_mapping import Preprint

# this is historical data from OSF Preprints, downloaded via the API,
# containing 57,730 articles from 2016-07-13 to 2020-06-31
input_files = glob.glob("data/osf-*.pkl")

# move files starting at "beginning" (i.e., with no start date) to
# the front of the list
input_files = sorted(input_files, key=lambda x: "data/osf-00" if "beginning" in x else x)


# it would be better to add the provider as another embed when making the
# API requests, but at the point I realized this, I had already pulled
# like half the historical data, so this is the solution I'm going with...
def get_provider(url):
    providers = {
        "psyarxiv": "PsyArXiv",
        "socarxiv": "SocArXiv",
        "engrxiv": "engrXiv",
        "lawarxiv": "LawArXiv",
        "inarxiv": "INA-Rxiv",
        "eartharxiv": "EarthArXiv",
        "paleorxiv": "PaleorXiv",
        "sportrxiv": "SportRxiv",
        "thesiscommons": "Thesis Commons",
        "lissa": "LIS Scholarship Archive",
        "mindrxiv": "MindRxiv",
        "metaarxiv": "MetaArXiv",
        "marxiv": "MarXiv",
        "agrixiv": "AgriXiv",
        "focusarchive": "FocUS Archive",
        "nutrixiv": "NutriXiv",
        "arabixiv": "Arabixiv",
        "ecsarxiv": "ECSarXiv",
        "africarxiv": "AfricArXiv",
        "ecoevorxiv": "EcoEvoRxiv",
        "frenxiv": "Frenxiv",
        "mediarxiv": "MediArXiv",
        "bodoarxiv": "BodoArXiv",
        "edarxiv": "EdArXiv",
        "indiarxiv": "IndiaRxiv",
        "biohackrxiv": "BioHackrXiv",
    }
    provider = "Unknown"

    for k, v in providers.items():
        if k in url:
            provider = v

    if provider == "Unknown" and url.startswith("https://api.osf.io/v2/providers/preprints/osf/"):
        provider = "OSF"
    return provider


def get_authors(contributors):
    authors = []
    for c in contributors["data"]:
        if c["attributes"]["bibliographic"]:
            if "data" in c["embeds"]["users"]:
                authors.append(c["embeds"]["users"]["data"]["attributes"]["full_name"])
            else:
                # happens when user has been deleted for some reason
                authors.append(c["embeds"]["users"]["errors"][0]["meta"]["full_name"])
    return authors


elastic_host = f"{os.getenv('ELASTIC_ADMIN_USER')}:{os.getenv('ELASTIC_ADMIN_PASS')}@{os.getenv('ELASTIC_HOST')}"
connections.create_connection(hosts=[elastic_host], timeout=20)

documents = []
i = 0
for fl in input_files:
    with open(fl, "rb") as f:
        data_block = pickle.load(f)

    for item in data_block:
        data = item["data"]
        pub_date = data["attributes"]["date_published"]
        if pub_date is None:
            pub_date = data["attributes"]["date_created"]

        # turns hierarchy of categories into a flat list of categories,
        # with lower-level categories denoted by " > "
        # e.g., "Business > Accounting"
        all_categories = data["attributes"]["subjects"]
        categories = []
        for group in all_categories:
            chain = [c["text"] for c in group]
            text = " > ".join(chain[:2])  # max. 2 levels
            categories.append(text)

        preprint = Preprint(
            url=data["links"]["html"],
            source=get_provider(data["relationships"]["provider"]["links"]["related"]["href"]),
            source_id=data["id"],
            publish_date=datetime.fromisoformat(pub_date),
            modified_date=datetime.fromisoformat(data["attributes"]["date_modified"]),
            stored_date=datetime.now(),
            title=data["attributes"]["title"],
            # TODO: Need to figure out how to deal with LaTeX code
            abstract=data["attributes"]["description"],
            # TODO: Format authors appropriately;
            # also deal with special characters
            authors=get_authors(data["embeds"]["contributors"]),
            categories=categories
        )
        documents.append(preprint)
        i += 1
        if i % 1000 == 0:
            print(f"Adding {len(documents)} documents ({i} so far) to database [{datetime.now()}]")
            bulk(connections.get_connection(), (d.to_dict(True) for d in documents))
            documents = []

print(f"Adding {len(documents)} documents ({i} so far) to database [{datetime.now()}]")
bulk(connections.get_connection(), (d.to_dict(True) for d in documents))