import requests
import json
import pandas as pd

token = 'your_token_here'
q_query = {your_query_here}

# not supported by API when code was writtne on 2022-10-18
# Actions,Modules,Days in stage, 

def getTotangoAccounts_andFields(token, query):
    def _s(v):
        return "" if v is None else str(v)

    dfAccounts = pd.DataFrame(columns=['AccountId'], dtype=str)
    known_cols = {'AccountId'}

    def ensure_col(name):
        if name not in known_cols:
            known_cols.add(name)
            dfAccounts[name] = pd.Series(dtype=str)

    url = "https://api.totango.com/api/v2/accounts/scan_and_scroll_search_accounts"
    headers = {
        'app-token': token,
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    scrollID = None
    offset = 0
    totalHits = 1
    listofAccounts = []
    while offset < totalHits:
        query['offset'] = offset          # was `+=` — that was double-incrementing
        payload = 'query=' + json.dumps(query)
        if scrollID:
            payload += '&scroll_id=' + scrollID
        response = requests.request("POST", url, headers=headers, data=payload)
        resp = json.loads(response.text)
        print(response.text)

        scrollID = resp['scroll-id']
        listofAccounts += resp['accounts']
        offset += 1000
        totalHits = resp['total_hits']

    for index, account in enumerate(listofAccounts):
        print('---------------')
        print(account)

        dfAccounts.at[index, 'AccountId'] = _s(account['id'])

        if "attributes" in account:
            for attribute in account['attributes']:
                ensure_col(attribute)
                dfAccounts.at[index, attribute] = _s(account['attributes'][attribute]['val'])

        if "metrics" in account:
            for metric in account['metrics']:
                m = account['metrics'][metric]
                metric_name = m.get('display_name', metric)
                if 'relative_changes' in m:
                    metric_name += ' percent changge'
                ensure_col(metric_name)
                if 'curr' in m:
                    dfAccounts.at[index, metric_name] = _s(m['curr'])
                elif 'relative_changes' in m:
                    for item in m['relative_changes']:
                        dfAccounts.at[index, metric_name] = _s(m['relative_changes'][item])
                if 'prev' in m:
                    col = metric + ' previous value'
                    ensure_col(col)
                    dfAccounts.at[index, col] = _s(m['prev'])
                if 'last_change_date' in m:
                    col = metric + ' last change date'
                    ensure_col(col)
                    dfAccounts.at[index, col] = _s(m['last_change_date'])

        if "named_aggregations" in account:
            for named_agg in account['named_aggregations']:
                na = account['named_aggregations'][named_agg]
                for agg in na:
                    if agg == 'display_name':
                        break
                    named_agg_name = na['display_name'] + ' ' + agg
                    ensure_col(named_agg_name)
                    dfAccounts.at[index, named_agg_name] = _s(na[agg]['curr'])

        if "display_name" in account:
            ensure_col('account_name')
            dfAccounts.at[index, 'account_name'] = _s(account['display_name'])

        if "health" in account:
            ensure_col('health')
            dfAccounts.at[index, 'health'] = _s(account['health']['curr'])

        if "status_group" in account:
            ensure_col('Contract Status')
            dfAccounts.at[index, 'Contract Status'] = _s(account['status_group'])

        if "status" in account:
            ensure_col('Status')
            dfAccounts.at[index, 'Status'] = _s(account['status'])

    print(dfAccounts)
    return dfAccounts

dataframe = getTotangoAccounts_andFields(token,q_query)
