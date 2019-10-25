import pandas as pd
import numpy as np
import multiprocessing as mp


def escaped_split(s, delim="|"):
    """function to delimit the provided string in the right manner taking care of escaoed delimiter"""

    ret = []
    current = []
    itr = iter(s)
    for ch in itr:
        if ch == '\\':
            try:
                # skip the next character; it has been escaped!
                current.append('\\')
                current.append(next(itr))
            except StopIteration:
                pass
        elif ch == delim:
            # split! (add current to the list and reset it)
            ret.append(''.join(current))
            current = []
        else:
            current.append(ch)
    ret.append(''.join(current))
    return ret


def compute_stats(user_df_in):
    """user_df_in dataframe contains data from a single user, and returns a dataframe containing stats from this user"""

    user_df_in = user_df_in.sort_values('date')
    user_df_in['signed_amount'] = user_df_in['amount'].astype(float) * user_df_in['type'].apply(lambda x: -1 if (x == 'debit') else 1)

    user_df_in_by_date = user_df_in[['date', 'signed_amount']]
    user_df_in_by_date = user_df_in_by_date.groupby('date').sum()

    user_df_out = pd.DataFrame()
    user_df_out['user_id'] = pd.Series(user_df_in.user_id.iloc[0])
    user_df_out['num_transactions'] = np.size(user_df_in.index)
    user_df_out['total_transaction_amount'] = np.sum(user_df_in.signed_amount)
    user_df_out['min_balance'] = round(user_df_in_by_date.cumsum().min()[0], 2)
    user_df_out['max_balance'] = max(0, round(user_df_in_by_date.cumsum().max()[0], 2))
    user_df_out.set_index('user_id', inplace=True)

    return user_df_out


def iter_chunk_by_id(file):
    """generator to read the csv in chunks of user_id records. Each next call of fenerator will give a df for a user"""

    csv_reader = pd.read_csv(file, compression='gzip', iterator=True, chunksize=1, header=0, error_bad_lines=False)
    first_chunk = csv_reader.get_chunk()
    id = int(first_chunk.iloc[0,0].split('|')[0])
    chunk = pd.DataFrame(first_chunk)
    for l in csv_reader:
        new_id = int(l.iloc[0,0].split('|')[0])
        if id == new_id:
            id = new_id
            chunk = chunk.append(l)
            continue
        id = new_id
        yield chunk
        chunk = pd.DataFrame(l)
    yield chunk


def process_csv(filename):
    """main function to read the csv and produce the resulting dataframe"""

    chunk_iter = iter_chunk_by_id(filename)
    user_df_out_all = pd.DataFrame()
    for chunk in chunk_iter:

        chunk['new'] = chunk['user_id|account_id|amount|desc|date|type|misc'].apply(escaped_split)
        new_chunk = pd.DataFrame(chunk.new.values.tolist(), index=chunk.index)
        new_chunk.rename({0: 'user_id', 1: 'account_id', 2: 'amount', 3: 'desc', 4: 'date', 5: 'type', 6: 'misc'}, axis=1,
                         inplace=True)
        user_df_out = compute_stats(new_chunk)
        user_df_out_all = pd.concat([user_df_out, user_df_out_all])
        print(chunk)
        print("_____")

    return user_df_out_all


if __name__ == '__main__':

    files = ['data/transactions1.csv.gz',
             'data/transactions2.csv.gz',
             'data/transactions3.csv.gz']
    #proess the 3 files in parallel. Use thread count as minimum of the num files and cpu count

    pool = mp.Pool(processes=min(mp.cpu_count() - 1,len(files)))
    results = pool.map(process_csv, files)
    pool.close()
    pool.join()

    results_all = pd.concat(results)
    results_all.reset_index(inplace=True)
    results_all['user_id,n,sum,min,max'] = pd.Series(results_all.astype(str).values.tolist()).str.join(',')
    results_all[['user_id,n,sum,min,max']].sort_index().to_csv('data/solution.csv')

/*
def iter_chunk_by_id(file):
    """generator to read the csv in chunks of user_id records. Each next call of fenerator will give a df for a user"""

    csv_reader = pd.read_csv(file, compression='gzip', iterator=True, chunksize=10000, header=0, error_bad_lines=False)
    chunk = pd.DataFrame()
    for l in csv_reader:
        l[['id', 'everything_else']] = l[
            'user_id|account_id|amount|desc|date|type|misc'].str.split('|', 1, expand=True)
        hits = l['id'].astype(float).diff().dropna().nonzero()[0]
        if not len(hits):
            # if all ids are same
            chunk.append(l[['user_id|account_id|amount|desc|date|type|misc']])
        else:
            start = 0
            for index,id in enumerate(hits):
                new_id = hits[index]
                chunk.append(l[['user_id|account_id|amount|desc|date|type|misc']].iloc[start:id, :])
                yield chunk
                start = new_id
                chunk = pd.DataFrame(chunk[['user_id|account_id|amount|desc|date|type|misc']].iloc[new_id:, :])
    yield chunk
  */
