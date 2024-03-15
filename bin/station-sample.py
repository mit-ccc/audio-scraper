import time
import random
import logging

import numpy as np
import pandas as pd
import requests as rq

from tqdm import tqdm

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    def _check_url(url):
        try:
            with rq.get(url, stream=True, timeout=10) as resp:
                return resp.status_code
        except rq.exceptions.RequestException:
            return -1

    fmts = ['Public Radio', 'News/Talk', 'College', 'Talk',
            'Business News', 'News']

    # list of all stations; untar from s3://lsm-data/talk-radio/radio.tar.gz
    df = pd.read_csv('main.csv', sep='\t')

    df['is_public'] = df['format'] == 'Public Radio'
    df = df.loc[df['format'].isin(fmts)]
    df = df.loc[df['stream_url'].notna()]
    df = df.loc[~df['stream_url'].str.contains('radio-locator')]

    cells = df.groupby(['state', 'is_public']).size().index.tolist()

    rows = []
    for state, is_public in tqdm(cells):
        eligible = df.loc[
            (df['state'] == state) &
            (df['is_public'] == is_public)
        ].sample(frac=1)

        for index, row in tqdm(eligible.iterrows()):
            status = _check_url(row['stream_url'])
            if status == 200:
                rows += [index]
                break
            time.sleep(2)

    samp = df.loc[df.index.isin(rows)].sample(frac=1)

    samp['source_id'] = np.arange(samp.shape[0])
    samp['name'] = samp['callsign'] + '-' + samp['band']
    samp['auto_ingest'] = True
    samp['lang'] = 'en'
    samp['retry_on_close'] = True

    source_data = samp[['source_id', 'name', 'stream_url', 'auto_ingest',
                        'lang', 'retry_on_close']]

    source_data.to_csv('source-data.csv', sep='\t', index=False)
    samp.to_csv('source-data-full.csv', sep='\t', index=False)
