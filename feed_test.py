# %%
import feedparser as fp
import json
import re
import time
import pandas as pd
import io
# %%
#dictionary to convert US State Department Country Codes to ISO2 as needed. 
BT_to_ISO ={
    'AG':'DZ', 'AN':'AD', 'AV':'AI', 'AY':'AQ', 'AC':'AG', 
    'AA':'AW', 'AS':'AU', 'AU':'AT', 'AJ':'AZ', 'BA':'BH',
    'BG':'BD', 'BO':'BY', 'BH':'BZ', 'BN':'BJ', 'BD':'BM',
    'BOLIV':'BO', 'A1':'BQ', 'BK':'BA', 'BC':'BW', 
    'VI':'VG', 'BX':'BN', 'BU':'BG', 'UV':'BF', 
    'BM':'MM', 'BY':'BI', 'CB':'KH', 'CJ':'KY', 'CT':'CF',
    'CD':'TD', 'CI':'CL', 'CH':'CN', 'CN':'KM', 'CS':'CR',
    'IV':'CI', 'UC':'CW', 'EZ':'CZ', 'CG':'CD', 'DO':'DM', 
    'DR':'DO', 'ES':'SV', 'EK':'GQ', 'EN':'EE', 'WZ':'SZ', 
    'A2':'GF', 'FP':'PF', 'GB':'GA', 'GG':'GE', 'GM':'DE', 
    'GJ':'GD', 'GV':'GN', 'HA':'HT', 'HO':'HN', 'IC':'IS', 
    'IZ':'IQ', 'EI':'IE', 'JA':'JP', 'DA':'DK', 'KR':'KI', 
    'KV':'XK', 'KU':'KW', 'LG':'LV', 'LE':'LB', 'LT':'LS', 
    'LI':'LR', 'LS':'LI', 'LH':'LT', 'MA':'MG', 'MI':'MW', 
    'RM':'MH', 'MP':'MU', 'MG':'MN', 'MJ':'ME', 'MH':'MS', 
    'MO':'MA', 'WA':'NA', 'NU':'NI', 'NG':'NE', 'NI':'NG', 
    'KN':'KP', 'MU':'OM', 'PS':'PW', 'PM':'PA', 'PP':'PG', 
    'PA':'PY', 'RP':'PH', 'PO':'PT', 'CF':'CG', 'RS':'RU', 
    'SC':'KN', 'ST':'LC', 'TP':'ST', 'IS':'IL', 'SG':'SN', 
    'RI':'RS', 'SE':'SC', 'SN':'SG', 'NN':'SX', 'LO':'SK', 
    'BP':'SB', 'SF':'ZA', 'KS':'KR', 'OD':'SS', 'SP':'ES', 
    'CE':'LK', 'SU':'SD', 'NS':'SR', 'SW':'SE', 'SR':'CH', 
    'TI':'TJ', 'BF':'BS', 'GA':'GM', 'TT':'TL', 'TO':'TG', 
    'TN':'TO', 'TD':'TT', 'TS':'TN', 'TU':'TR', 'TX':'TM', 
    'TK':'TC', 'UP':'UA', 'UK':'GB', 'NH':'VU', 'VM':'VN', 
    'YM':'YE', 'ZA':'ZM', 'ZI':'ZW', 'KO':'KR'
    }

def ISO_convert(BT):
    val = BT_to_ISO.get(BT, BT)
    return val 

# %%
def rss_to_json(rss_url):
    feed = fp.parse(rss_url)

    if feed.status == 200:
        data =[]
        for entry in feed.entries:
            record = {
                'Name': entry['title'].split(' - ')[0],
                'pubDate': time.strftime(r'%m/%d/%Y',entry['published_parsed']),
                'ISO_A2': ISO_convert('BOLIV' if 'BL' == entry['tags'][1]['term'] else entry['tags'][1]['term']), #the BL conversion is nessesary as the US country tags BL for bolivia was causing issues during the ISO2 conversion process for reasons not recalled by the author
                #in earlier version of this feed, BT was the term used for US State Department Country codes. This is retaind here as these codes still have to be recoded to ISOA2
                'Threat-Level': entry['tags'][0]['term']
            } 
            data.append(record)
        output = json.dumps(data)
        return output
    else:
        print(f'Failed to parse RSS feed from {rss_url}. Status code: {feed.status}')
        return None

# %%
feed = rss_to_json(r'https://travel.state.gov/_res/rss/TAsTWs.xml')
df =pd.read_json(io.StringIO(feed))
df['pubDate'] = pd.to_datetime(df.pubDate)
df.loc[df['pubDate'] > pd.Timestamp.today(), ['pubDate']] = pd.Timestamp.today()
df.loc[df['Name'] == 'Macau', ['ISO_A2']] = 'MO'
df.loc[df['Name'] == 'Hong Kong', ['ISO_A2']] = 'HK'
FT = df.loc[df.ISO_A2=='A3'].squeeze()
FT_df = pd.DataFrame({'pubDate':[FT.iloc[1]]*4, 'ISO_A2':['GP', 'MQ', 'MF', 'BL'], 'Threat-Level':[FT.iloc[3]]*4})
df = pd.concat([df, FT_df], ignore_index=True)
df.drop(df[df['ISO_A2'] == 'A3'].index, inplace=True)
df.drop('Name', axis=1, inplace=True)

df.head()
# %% 

# %%
old_data = pd.read_json(r'USSD_TAs.json')
old_data['pubDate'] = pd.to_datetime(old_data.pubDate)
old_data.head()
filtered_data = pd.merge(df, old_data.groupby('ISO_A2')['pubDate'].max(), on='ISO_A2', how='left').sort_values(by=['ISO_A2'])
filtered_data.loc[~(filtered_data['pubDate_x'] ==filtered_data['pubDate_y'])]
filtered_data.drop('pubDate_y', axis=1, inplace=True)
filtered_data.rename(columns={'pubDate_x': 'pubDate'}, inplace=True)
# %%
#filtered_data['pubDate'] = filtered_data['pubDate'].dt.strftime(r'%m/%d/%Y')
#old_data['pubDate'] = old_data['pubDate'].dt.strftime(r'%m/%d/%Y')
pd.concat([filtered_data, old_data], ignore_index=True).to_csv('USSD_TAS.csv', index=False, )

#= filtered_data.to_json(orient='records', lines=True)
#with open('USSD_TAs.json') as file:
    #old_j  = json.load(file)
#data = new_data + old_j
#data
# %%
new_data
# %%
old_j
# %%
filtered_data
# %%
