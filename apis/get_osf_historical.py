"""Queries OSF Preprints API for getting bulk historical data from OSF

This provides two functions that can be used to query from the API, but
is primarily meant to be run directly as a script rather than imported.
Run `python get_osf_historical.py --help` for more details about the
command-line arguments that can be passed.

Note that this is meant to be run as a one-time process in order to get
the historical data, then save it to disk so that we only need to query
the API once to get it.
"""

import requests
import json
import datetime
from datetime import datetime as dt
import time

PREPRINT_LIST = "https://api.osf.io/v2/preprints/?version=2.20&fields[preprints]="
PREPRINT_LIST_START_DATE_SUFFIX = "&filter[date_created][gte]={date}"
PREPRINT_LIST_END_DATE_SUFFIX = "&filter[date_created][lte]={date}"

PREPRINT_DATA_SUFFIX = "?version=2.0&embed=contributors"


def get_preprint_urls(start_date=None, end_date=None, max_results=None, osf_token=None):
    """Get the API endpoints for all preprints in OSF. If `start_date` or
    `end_date` are specified, will query only for preprints created
    between those dates. Both dates are inclusive. Dates must be
    specified in YYYY-MM-DD format. If `max_results` will only return at
    most that many results. If given an OSF token, this will be used to
    authenticate the requests, which allows for more requests per
    hour/day.
    """

    url = PREPRINT_LIST
    if start_date is not None:
        url += PREPRINT_LIST_START_DATE_SUFFIX.format(date=start_date)
    if end_date is not None:
        url += PREPRINT_LIST_END_DATE_SUFFIX.format(date=end_date)

    preprint_urls = []

    if osf_token is not None:
        headers = { "Authorization": f"Bearer {osf_token}" }
        req = requests.get(url, headers=headers)
    else:
        req = requests.get(url)
    num_requests = 1

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

    # response should have a "meta" key that provides more information
    # about pagination of results
    if "meta" in json_data:
        total = json_data["meta"]["total"]
        per_page = json_data["meta"]["per_page"]
        if max_results is not None:
            pages = max_results // per_page + (max_results % per_page > 0)
        else:
            pages = total // per_page + (total % per_page > 0)
        next_page = json_data["links"]["next"]

        print(f"Total pages: {pages}")
        print(f"Total records: {total}")

        page_count = 2  # we already parsed page 1 above
        while next_page is not None:
            req = requests.get(next_page)
            num_requests += 1

            if req.status_code != 200:
                print(f"Request error: {req.status_code}")
                print(dt.now())

                if req.status_code != 429:
                    print(req.headers)
                print(f"Total requests: {num_requests}")

                if req.status_code == 429:
                    delay = float(req.headers['Retry-After'])
                    print(f"Retry at: {dt.now() + datetime.timedelta(seconds=delay)}")
                    time.sleep(delay + 0.5)
                    continue
                else:
                    exit()

            json_data = json.loads(req.text)

            if "data" not in json_data:
                print(f"Page {page_count}: No data")
                continue

            # grab API endpoints for individual preprints
            for d in json_data["data"]:
                preprint_urls.append(d["links"]["self"])

            if page_count % 10 == 0:
                print(f"Page {page_count}: {len(preprint_urls)} [{dt.now()}]")

            if ((max_results is None or len(preprint_urls) < max_results)
                and "next" in json_data["links"]
                and json_data["links"]["next"] is not None):

                next_page = json_data["links"]["next"]
                page_count += 1
            else:
                next_page = None

    if max_results is not None:
        preprint_urls = preprint_urls[:max_results]
    print(f"Num. records: {len(preprint_urls)}")
    return (preprint_urls, num_requests)


def get_preprints(preprint_urls, osf_token=None, num_requests=0):
    """For each URL in list `preprint_urls`, make request to get metadata
    associated with that preprint. If given an OSF token, this will be used to authenticate the request, which allows for more requests per
    hour/day."""

    if len(preprint_urls) == 0:
        print("No preprints to download.")
        exit()

    data = []
    i = 0
    next_page = preprint_urls[0]
    while next_page is not None:
        url = next_page + PREPRINT_DATA_SUFFIX
        req = requests.get(url)
        num_requests += 1
        if req.status_code != 200:
            print(f"Request error: {req.status_code}")
            print(dt.now())

            if req.status_code != 429:
                print(req.headers)
            print(url)
            print(f"Total requests: {num_requests}")

            if req.status_code == 429:
                delay = float(req.headers['Retry-After'])
                print(f"Retry at: {dt.now() + datetime.timedelta(seconds=delay)}")
                time.sleep(delay + 0.5)
                continue
            else:
                i += 1
                if i < len(preprint_urls):
                    next_page = preprint_urls[i]
                else:
                    next_page = None
                continue

        data.append(json.loads(req.text))
        if (i+1) % 50 == 0:
            print(i+1)

        i += 1
        if i < len(preprint_urls):
            next_page = preprint_urls[i]
        else:
            next_page = None
    return (data, num_requests)


if __name__ == "__main__":
    import os
    from pathlib import Path
    import argparse
    import pickle

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", type=str, nargs="?",
        help="Start date to filter preprints by creation date. Must be in YYYY-MM-DD format.")
    parser.add_argument("-e", "--end", type=str, nargs="?",
        help="End date to filter preprints by creation date. Must be in YYYY-MM-DD format.")
    parser.add_argument("-m", "--max", type=int, nargs="?",
        help="Maximum number of results to return.")
    parser.add_argument("-t", "--token", type=str, nargs="?", default=os.getenv("OSF_TOKEN"),
        help="Personal authentication token used for making authenticated requests to OSF. May be set here or via the $OSF_TOKEN environment variable.")
    args = parser.parse_args()

    if args.start is None and args.end is None and args.max is None:
        print("Must specify at least one of start date, end date, or max. There are too many preprints to request all of them at once without running into rate limiting.")
        exit(1)

    if args.token is None:
        print("Warning: Authenticated requests get a higher number of requests per day. You should consider creating a personal authentication token and providing it with the environment variable $OSF_TOKEN.")

    preprint_urls, num_requests = get_preprint_urls(start_date=args.start, end_date=args.end, max_results=args.max, osf_token=args.token)
    data, num_requests = get_preprints(preprint_urls, osf_token=args.token, num_requests=num_requests)

    print(f"Total requests: {num_requests}")

    print("Saving data...")

    start_date = args.start if args.start is not None else "beginning"
    end_date = args.end if args.end is not None else "current"
    has_max = f"-max{args.max}" if args.max is not None else ""
    time = dt.strftime(dt.now(), "%Y%m%d-%H%M%S")

    curr_dir = Path(os.path.dirname(os.path.realpath(__file__)))
    out_dir = os.path.join(curr_dir.parent, "data")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, f"osf-{start_date}-to-{end_date}{has_max}_{time}.pkl")

    with open(out_file, "wb") as f:
        pickle.dump(data, f)
