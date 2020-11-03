"""Create TF-IDF of all articles, with a test set held out"""

import re
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
# nltk.download('stopwords')
# nltk.download('punkt')
# nltk.download('wordnet')

stop_words = set(stopwords.words("english"))
wnl = WordNetLemmatizer()


def parse_args():
    """Parse command-line arguments."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--modelfile",
        default="tf_idf_model.pkl", nargs="?",
        help="Filename where trained model will be saved (default tf_idf_model.pkl)")
    parser.add_argument("-o", "--outfile",
        default="tf_idf_test_set.pkl", nargs="?",
        help="Filename where document embeddings for test set will be saved (default tf_idf_test_set.pkl)")
    parser.add_argument("--testprop", default=0.1, nargs="?", type=float,
        help="Proportion of documents to include in test set (default 0.1)")
    parser.add_argument("--max_features", nargs="?", type=int,
        help="If set, vocabulary will consider only top `max_features` ordered by term frequency in training set")
    parser.add_argument("--min_df", nargs="?",
        help="If set, terms with document frequency lower than `min_df` will be ignored; if this is float between 0.0 and 1.0, parameter represents proportion of documents; if integer, represents absolute count")
    parser.add_argument("--max_df", nargs="?",
        help="If set, terms with document frequency higher than `max_df` will be ignored; if this is float between 0.0 and 1.0, parameter represents proportion of documents; if integer, represents absolute count")
    args = parser.parse_args()

    def min_max_convert(arg, name):
        """Ensures that argument is either a float in range [0.0, 1.0]
        or an integer."""
        if arg is not None:
            try:
                arg = float(arg)
                if arg > 1.0 or arg < 0.0:
                    sys.exit(f"Error: Argument `{name}` was not in range [0.0, 1.0].")
            except ValueError:
                try:
                    arg = int(arg)
                except ValueError:
                    sys.exit(f"Error: Argument `{name}` is not a valid float or integer.")
        return arg

    args.min_df = min_max_convert(args.min_df, "min_df")
    args.max_df = min_max_convert(args.max_df, "max_df")
    return args


def time_elapsed(seconds):
    """Converts a time in seconds to a human-readable time."""
    time = seconds
    unit = "s"
    if time >= 60:
        time /= 60
        unit = "m"
    if time >= 60:
        time /= 60
        unit = "h"
    return f"{time:.2f}{unit}"


def is_number(s):
    """Checks if a string is numeric."""
    try:
        float(s)
        return True
    except ValueError:
        return False


def tokenize(document):
    """Given a document, this tokenizes it into words; converts to
    lowercase; removes LaTeX expressions, stop words, single characters,
    punctuation, and numbers; and lemmatizes the words. Returns a list
    of tokens.
    """
    text = document.lower()
    words = word_tokenize(text, language="english")

    # remove LaTeX expressions, stop words, single characters,
    # punctuation, and numbers
    filtered = []
    in_latex = False
    for w in words:
        if w == '$':
            in_latex = not in_latex
        elif not in_latex:
            if (len(w) > 1
                and not w.startswith("\\")
                and not w in stop_words
                and not is_number(w)
                and re.match(r"^[0-9]", w) is None
                and re.match(r"^[!'\"#$%&()*+-./:;<=>?@[\]^_`{|}~]+$", w) is None):
                filtered.append(wnl.lemmatize(w))
    return filtered

def tokenizer(x):
    """This is just because the model can't be pickled when it includes
    a lambda for the tokenizer."""
    return tokenize(x.abstract)

def iterate_inputs(iter, max=None, verbose=True):
    """Provides a generator wrapper around an iterable that yields up to
    `max` number of results.
    """
    i = 0
    while max is None or i < max:
        if verbose and i % 1000 == 0:
            print(i)
        yield next(iter)
        i += 1


if __name__ == "__main__":
    import os
    import sys
    import time
    import pickle

    from elasticsearch_dsl import connections

    # to allow importing from parent directory
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from elastic.elastic_mapping import Preprint

    args = parse_args()

    elastic_host = f"{os.getenv('ELASTIC_ADMIN_USER')}:{os.getenv('ELASTIC_ADMIN_PASS')}@{os.getenv('ELASTIC_HOST')}"
    connections.create_connection(hosts=[elastic_host], timeout=20)

    total = Preprint.search().count()
    test_set_start_idx = total - int(total * args.testprop)
    print(f"Total: {total}")
    print(f"Training set: {test_set_start_idx - 1}")
    print(f"Test set: {total - test_set_start_idx - 1}")

    search = Preprint.search().sort('publish_date')
    search._params.update(scroll="1m", size="10000")
    search_iter = search.scan()

    kwargs = {}
    if args.max_features:
        kwargs["max_features"] = args.max_features
    if args.min_df:
        kwargs["min_df"] = args.min_df
    if args.max_df:
        kwargs["max_df"] = args.max_df

    model = TfidfVectorizer(
        lowercase=False,
        tokenizer=tokenizer,
        token_pattern=None,
        **kwargs)

    print("Training model...")
    start_time = time.time()
    model.fit(iterate_inputs(search_iter, max=test_set_start_idx))
    print(f"Completed in {time_elapsed(time.time() - start_time)}")

    print("Gathering test set...")
    start_time = time.time()
    test_set = [item for item in search_iter]
    print(f"Completed in {time_elapsed(time.time() - start_time)}")
    print(f"Test set size: {len(test_set)}")  # 176,118

    print("Creating embeddings for test set...")
    start_time = time.time()
    tf_idf = model.transform(test_set)
    print(f"Completed in {time_elapsed(time.time() - start_time)}")

    print("Saving model and test set output...")
    with open(args.modelfile, "wb") as f:
        pickle.dump(model, f)

    with open(args.outfile, "wb") as f:
        pickle.dump((test_set, tf_idf), f)
    print("Complete!")
