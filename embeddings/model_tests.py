"""Evaluates the test set of various models. Currently calculates the
average cosine similarity for each category of document in the test
set."""

import os
import sys
import pickle
import random
import tempfile

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


# to allow importing from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from elastic.elastic_mapping import Preprint

models = ["tf_idf"]
num_random = 1000


def avg_distance(embeddings):
    """Calculates the average cosine similarity of embeddings.
    `embeddings` should be a numpy array -- either ndarray or sparse
    array.
    """
    dist = cosine_similarity(embeddings)
    diag = np.tril(dist, -1)
    sum_val = np.count_nonzero(diag)
    if sum_val == 0:
        return np.nan
    return np.sum(diag) / sum_val


for model in models:
    with open(f"{model}_test_set.pkl", "rb") as f:
        data, embeddings = pickle.load(f)

    # this is a bit of a workaround because I couldn't fit everything
    # in memory at the same time; we just read things into temp files
    # as we're doing the calculations, and then read them back out at
    # the end
    with tempfile.TemporaryDirectory() as tmpdir:
        # get average of random items
        n = embeddings.shape[0]
        random.seed(42)
        idx = random.sample(range(0, n), k=num_random)
        avg_dist = avg_distance(embeddings[idx, :])
        with open(os.path.join(tmpdir, "random"), "w") as f:
            f.write(str(avg_dist.item()))

        # find all the ids for each category
        categories = {}
        for i, preprint in enumerate(data):
            for cat in preprint.categories:
                if cat not in categories:
                    categories[cat] = []
                categories[cat].append(i)

        for cat, idx in categories.items():
            avg_dist = avg_distance(embeddings[idx, :])
            with open(os.path.join(tmpdir, cat), "w") as f:
                if np.isnan(avg_dist):
                    f.write("nan")
                else:
                    f.write(str(avg_dist.item()))

        with open(f"{model}_cosine_test_set.txt", "w") as outfile:
            categories["random"] = list(range(num_random))
            for cat in categories.keys():
                with open(os.path.join(tmpdir, cat), "r") as f:
                    outfile.write(f"{float(f.read()):4f}\t{len(categories[cat])}\t{cat}\n")