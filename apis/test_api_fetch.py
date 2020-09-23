"""Misc. functions to test querying from arXiv and OSF APIs"""

import requests
import xmltodict
import time
import json
import pickle


def get_arxiv(date):
    url = f"http://export.arxiv.org/oai2?verb=ListRecords&metadataPrefix=arXiv&from={date}"
    req = requests.get(url)

    if req.status_code != 200:
        print(f"Request error: {req.status_code}")
        print(req.headers)
        exit()

    doc = xmltodict.parse(req.text)
    list_records = doc['OAI-PMH']['ListRecords']
    data = list_records['record']

    if 'resumptionToken' in list_records and '#text' in list_records['resumptionToken']:
        token = list_records['resumptionToken']['#text']
        cursor = list_records['resumptionToken']['@cursor']
        list_size = list_records['resumptionToken']['@completeListSize']
        resume = True
        # loop through each page of results
        while resume:
            url = f"http://export.arxiv.org/oai2?verb=ListRecords&resumptionToken={token}"
            req = requests.get(url)

            if req.status_code == 503:
                delay = req.headers['Retry-After']
                time.sleep(float(delay) + 0.5)
                continue
            elif req.status_code != 200:
                print(f"Request error: {req.status_code}")
                print(req.headers)
                exit()

            doc = xmltodict.parse(req.text)
            list_records = doc['OAI-PMH']['ListRecords']
            data += list_records['record']

            if 'resumptionToken' in list_records and '#text' in list_records['resumptionToken']:
                token = list_records['resumptionToken']['#text']
                cursor = list_records['resumptionToken']['@cursor']
                print(f"{cursor} / {list_size}")
                time.sleep(5)
            else:
                resume = False

    print("Saving data...")
    with open("../data/test_arxiv.pkl", "wb") as f:
        pickle.dump(data, f)

def read_arxiv():
    with open("../data/test_arxiv.pkl", "rb") as f:
        data = pickle.load(f)

    print(f"Num. records: {len(data)}")
    for rec in data[0:10]:
        # remove weird newlines in titles
        title = rec['metadata']['arXiv']['title'].splitlines()
        title = ' '.join([t.strip() for t in title])
        print(title)

    # unique = []
    # dupes = []
    # for rec in data:
    #     aid = rec['metadata']['arXiv']['id']
    #     if aid in unique:
    #         dupes.append(aid)
    #     else:
    #         unique.append(aid)
    # print(f"Unique: {len(unique)}")
    # print(f"Duplicates: {len(dupes)}")


def get_osf(date):
    # just get the very basic data for making individual requests for
    # each page, and later each preprint
    url = f"https://api.osf.io/v2/preprints/?version=2.20&filter[date_modified][gte]={date}&fields[preprints]="
    preprint_urls = []

    req = requests.get(url)
    if req.status_code != 200:
        print(f"Request error: {req.status_code}")
        print(req.headers)
        exit()

    json_data = json.loads(req.text)

    if "data" not in json_data:
        print("Request had no data associated with it.")
        exit()

    # grab API endpoints for individual preprints
    for d in json_data["data"]:
        preprint_urls.append(d["links"]["self"])

    if "meta" in json_data:
        total = json_data["meta"]["total"]
        per_page = json_data["meta"]["per_page"]
        pages = total // per_page + (total % per_page > 0)
        next_page = json_data["links"]["next"]

        print(f"Total pages: {pages}")

        for _ in range(pages):
            req = requests.get(next_page)
            if req.status_code != 200:
                print(f"Request error: {req.status_code}")
                print(req.headers)
                break

            json_data = json.loads(req.text)

            if "data" not in json_data:
                continue

            # grab API endpoints for individual preprints
            for d in json_data["data"]:
                preprint_urls.append(d["links"]["self"])
            
            if ("next" in json_data["links"]
                and json_data["links"]["next"] is not None):
                next_page = json_data["links"]["next"]
            else:
                break

    print(f"Num. records: {len(preprint_urls)}")

    # now make individual requests for each preprint separately
    data = []
    for i, url in enumerate(preprint_urls):
        url = url + "?version=2.0&embed=contributors"
        req = requests.get(url)
        if req.status_code != 200:
            print(f"Request error: {req.status_code}")
            print(req.headers)
            print(url)
            continue

        data.append(json.loads(req.text))
        if i % 50 == 0:
            print(i)

    print("Saving data...")
    with open("../data/test_osf.pkl", "wb") as f:
        pickle.dump(data, f)

def read_osf():
    with open("../data/test_osf.pkl", "rb") as f:
        data = pickle.load(f)
    print(f"Num. records: {len(data)}")
    for d in data[0:10]:
        print(d['data']['attributes']['title'])

if __name__ == "__main__":
    # these are just the dates I happened to use, there's nothing
    # special about them
    date_arxiv = "2020-08-27"
    date_osf = "2020-09-06"

    # get_arxiv(date_arxiv)
    read_arxiv()

    # get_osf(date_osf)
    # read_osf()