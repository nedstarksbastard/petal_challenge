import pandas as pd


def iter_chunk_by_id(file_name, chunk_size=10000):
    """generator to read the csv in chunks of user_id records. Each next call of generator will give a df for a user"""

    csv_reader = pd.read_csv(file_name, compression='gzip', iterator=True, chunksize=chunk_size, header=0, error_bad_lines=False)
    chunk = pd.DataFrame()
    for l in csv_reader:
        l[['id', 'everything_else']] = l[
            'user_id|account_id|amount|desc|date|type|misc'].str.split('|', 1, expand=True)
        hits = l['id'].astype(float).diff().dropna().nonzero()[0]
        if not len(hits):
            # if all ids are same
            chunk = chunk.append(l[['user_id|account_id|amount|desc|date|type|misc']])
        else:
            start = 0
            for i in range(len(hits)):
                new_id = hits[i]+1
                chunk = chunk.append(l[['user_id|account_id|amount|desc|date|type|misc']].iloc[start:new_id, :])
                yield chunk
                chunk = pd.DataFrame()
                start = new_id
            chunk = l[['user_id|account_id|amount|desc|date|type|misc']].iloc[start:, :]

    yield chunk


