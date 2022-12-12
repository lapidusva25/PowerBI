import requests
import json
import pandas as pd

token = '<yourtokenhere>'
q_query = {your_query}

# not supported by API when code was writtne on 2022-10-18
# Actions,Modules,Days in stage, 

def getTotangoAccounts_andFields(token,query):
  #currently only supports health/attributes/display name, and ID, needs to expand to support
  #named_aggregations
  #metrics, and others
  dtype_columns = {}
  dfAccounts = pd.DataFrame(columns = ['AccountId'], dtype = str)
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
    query['offset']+=offset
    payload='query=' + json.dumps(query)
    if(scrollID):
      payload+='&scroll_id=' + scrollID
    response = requests.request("POST",url, headers=headers, data=payload)
    resp = json.loads(response.text)
    print(response.text)
    scrollID = resp['scroll-id']
    listofAccounts += resp['accounts']
    offset+=1000
    totalHits = resp['total_hits']


  
  for index,account in enumerate(listofAccounts):
    print('---------------')
    print(account)
    # input('---------------')
    

    dfAccounts.at[index,'AccountId']=account['id']
    
    if "attributes" in account.keys():
      for attribute_index,attribute in enumerate(account['attributes']):
        if attribute not in dtype_columns:
          dtype_columns[attribute] = True
          dfAccounts[attribute] = pd.NaT
          dfAccounts[attribute]=dfAccounts[attribute].astype(str)
        dfAccounts.at[index,attribute]=account['attributes'][attribute]['val']

    if "metrics" in account.keys():
      for metric_index,metric in enumerate(account['metrics']): 
        if 'display_name' in account['metrics'][metric].keys():
          metric_name = account['metrics'][metric]['display_name']
        else:
          metric_name = metric
        if 'relative_changes' in account['metrics'][metric].keys():
          metric_name += ' percent changge'
        if metric_name not in dtype_columns:
          dtype_columns[metric_name] = True
          dfAccounts[metric_name] = pd.NaT
          dfAccounts[metric_name]=dfAccounts[metric_name].astype(str)
        if 'curr' in account['metrics'][metric].keys():
          dfAccounts.at[index,metric_name]=account['metrics'][metric]['curr']
        elif 'relative_changes' in account['metrics'][metric].keys():
          for item in account['metrics'][metric]['relative_changes']:
            dfAccounts.at[index,metric_name]=account['metrics'][metric]['relative_changes'][item]
        if 'prev' in account['metrics'][metric].keys():
          if metric + ' previous value' not in dtype_columns:
            dtype_columns[metric + ' previous value'] = True
            dfAccounts[metric + ' previous value'] = pd.NaT
            dfAccounts[metric + ' previous value']=dfAccounts[metric + ' previous value'].astype(str)
          dfAccounts.at[index,metric + ' previous value']=account['metrics'][metric]['prev']
        if 'last_change_date' in account['metrics'][metric].keys():
          if metric + ' last change date' not in dtype_columns:
            dtype_columns[metric + ' last change date'] = True
            dfAccounts[metric + ' last change date'] = pd.NaT
            dfAccounts[metric + ' last change date']=dfAccounts[metric + ' previous value'].astype(str)
          dfAccounts.at[index,metric + ' last change date']=account['metrics'][metric]['last_change_date']

    if "named_aggregations" in account.keys():
      for named_agg_index,named_agg in enumerate(account['named_aggregations']):
        
        for agg_index,agg in enumerate(account['named_aggregations'][named_agg]):
          if agg=='display_name':
            break
          named_agg_name = account['named_aggregations'][named_agg]['display_name'] + ' ' + agg
          if named_agg_name not in dtype_columns:
            dtype_columns[named_agg_name] = True
            dfAccounts[named_agg_name] = pd.NaT
            dfAccounts[named_agg_name]=dfAccounts[named_agg_name].astype(str)
          dfAccounts.at[index,named_agg_name]=account['named_aggregations'][named_agg][agg]['curr']

    if "display_name" in account.keys():
      if 'account_name' not in dtype_columns:
        dtype_columns['account_name'] = True
        dfAccounts['account_name'] = pd.NaT
        dfAccounts['account_name']=dfAccounts['account_name'].astype(str)
      dfAccounts.at[index,'account_name'] = account['display_name']

    if "health" in account.keys():
      if 'health' not in dtype_columns:
        dtype_columns['health'] = True
        dfAccounts['health'] = pd.NaT
        dfAccounts['health']=dfAccounts['health'].astype(str)
      dfAccounts.at[index,'health'] = account['health']['curr']

    if "status_group" in account.keys():
      if 'Contract Status' not in dtype_columns:
        dtype_columns['Contract Status'] = True
        dfAccounts['Contract Status'] = pd.NaT
        dfAccounts['Contract Status']=dfAccounts['Contract Status'].astype(str)
      dfAccounts.at[index,'Contract Status'] = account['status_group']

    if "status" in account.keys():
      if 'Status' not in dtype_columns:
        dtype_columns['Status'] = True
        dfAccounts['Status'] = pd.NaT
        dfAccounts['Status']=dfAccounts['Status'].astype(str)
      dfAccounts.at[index,'Status'] = account['status']


  print(dfAccounts)
  return dfAccounts


dataframe = getTotangoAccounts_andFields(token,q_query)
