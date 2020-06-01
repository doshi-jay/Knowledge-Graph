# import wptools
# from wik
from infobox.page import WPToolsPage as page
# from .page import WPToolsPage as page

from pprint import pprint
import requests
import warnings
warnings.filterwarnings('ignore')

queries = ["ElonMusk", "Elon Musk", "Elon_Musk", "Elon-Musk", "Elon%20Musk", "Elon_MUSK", "Elon   Musk"]

variations = []
for query in queries:
    variations.append(query.lower())
    variations.append(query.upper())

queries += variations


def check_queries(queries):
    valid, invalid = [], []
    for query in queries:
        try:
            result = requests.get('https://en.wikipedia.org/wiki/{0}'.format(query), verify=False)
        except Exception:
            pass

        if result.status_code == 200:  # the article exists
            valid.append(query)
        else:
            invalid.append((query, result.status_code))

    return valid, invalid


# valid, invalid = check_queries(queries)
# pprint(valid)
# pprint("=================================")
# pprint(invalid)



f = page(queries[1])
td = f.get_parse()

pprint(td)
