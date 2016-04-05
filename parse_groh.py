from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd
import phonenumbers


def list_fixer(x):
    return '|'.join(sorted(set(x.split('|'))))


def strip_us_phonenumbers(x):
    matches = phonenumbers.PhoneNumberMatcher(x, "US")
    for m in matches:
        x = x.replace(m.raw_string, '<US_PHONE_NUM>')
    return x

df = pd.read_csv('data/HTlabeled.csv')

# empty columns
del df['tip']
del df['hasBodyPart']

for col in ['dateCrawled', 'dateCreated', 'dateModified']:
    df.ix[:, col] = df.ix[:, col].apply(pd.to_datetime)

for col in ['emailaddress_feature',
            'person_age_feature',
            'person_ethnicity_feature',
            'phonenumber_feature']:
    df.ix[:, col] = df.ix[:, col].fillna('')

for col in ['person_age_feature',
            'person_ethnicity_feature',
            'phonenumber_feature']:
    df.ix[:, col] = df.ix[:, col].apply(list_fixer)

df.person_ethnicity_feature = df.person_ethnicity_feature.astype('category')

df.hasTitlePart = df.hasTitlePart.apply(lambda x: x.decode('latin1'))

df['title_no_phone'] = df.hasTitlePart.apply(strip_us_phonenumbers)

es = Elasticsearch(
    ['https://els.istresearch.com:19200'],
    http_auth=('memex', 'qRJfu2uPkMLmH9cp'), verify_certs=False)

es_query = {
    "query": {
        "filtered": {
            "filter": {
                "terms": {
                    "url.exact": df.url.unique().tolist()
                }
            }
        }
    },
    "fields": ["extracted_text", "extracted_metadata.title", "raw_content", "url"],
    "from": 0,
    "size": 2000 # hard-coded max. Call it a hack.
}


results = es.search(index=['memex-domains'], doc_type=['escorts'], body=es_query)

hits = [x['fields'] for x in results['hits']['hits']]
for i in range(len(hits)):
    for key in hits[i]:
        hits[i][key] = hits[i][key][0].strip()
    hits[i]['_id'] = results['hits']['hits'][i]['_id']
