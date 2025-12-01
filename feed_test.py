# %%
import feedparser as fp
import json
import re
import time
import pandas as pd
import io
import requests
from bs4 import BeautifulSoup
import geopandas as gpd
import zipfile
import matplotlib.pyplot as plt
# %%
#dictionary to convert US State Department Country Codes to ISO2 as needed. 
#in an earlier version of this RSS feed, BT was the term used for US State Department Country codes. This is retaind here as these codes still have to be recoded to ISOA2

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
                'Threat-Level': entry['tags'][0]['term'],
                'Threat-Num': int(re.search(r'\d+', entry['tags'][0]['term']).group())
            } 
            data.append(record)
        output = json.dumps(data)
        return output
    else:
        print(f'Failed to parse RSS feed from {rss_url}. Status code: {feed.status}')
        return None

#this function is used only if information is to be extracted from the state department website directly
def rating(line):
    if 'do no travel' in line.lower():
        level = 'Level 4: Do Not Travel'
    elif 'reconsider travel' in line.lower():
        level = 'Level 3: Reconsider Travel'
    elif 'increased caution' in line.lower():
        level ='Level 2: Exercise Increased Caution'
    elif 'normal precautions' in line.lower():
        level = 'level 1: exercise normal precautions'
    else:
        level = 'unkown error'
    return(level)
today = pd.Timestamp.today() 
# %%
# due to loading issues with the rss feed, a try and except block was nessesary
for attempt in range(5):
    try:
        feed = rss_to_json(r'https://travel.state.gov/_res/rss/TAsTWs.xml')
        df =pd.read_json(io.StringIO(feed))
        df['pubDate'] = pd.to_datetime(df.pubDate)
        df.loc[df['pubDate'] > today, ['pubDate']] = today #sometimes erronious publication dates are provided
        #Hong Kong and Macau are not given country codes in the RSS feed so have to be handled seperatly. 
        df.loc[df['Name'] == 'Macau', ['ISO_A2']] = 'MO'
        df.loc[df['Name'] == 'Hong Kong', ['ISO_A2']] = 'HK'
        #the next four lines are to handle the US State Department aggregating the French overseas territories under one designation
        FT = df.loc[df.ISO_A2=='A3'].squeeze()
        FT_df = pd.DataFrame({'pubDate':[FT.iloc[1]]*4, 'ISO_A2':['GP', 'MQ', 'MF', 'BL'], 'Threat-Level':[FT.iloc[3]]*4})
        df = pd.concat([df, FT_df], ignore_index=True)
        df.drop(df[df['ISO_A2'] == 'A3'].index, inplace=True)
        df.drop('Name', axis=1, inplace=True)

        if 'IL' in df['ISO_A2'].values:
            new_date = df.loc[df['ISO_A2']=='IS', 'pubDate']
            url = 'https://travel.state.gov/content/travel/en/traveladvisories/traveladvisories/israel-west-bank-and-gaza-travel-advisory.html'
            response =requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            spoon = soup.find('div',class_='tsg-rwd-emergency-alert-text')
            IL_r = rating(spoon.find_all('b', string= lambda t: t and 'Israel –' in t)[0].get_text())
            PS_r1 = rating(spoon.find_all('b', string= lambda t: t and 'West Bank –' in t)[0].get_text())
            PS_r2 = rating(spoon.find_all('b', string= lambda t: t and 'Gaza –' in t)[0].get_text())
            PS_rf = {
                1: 'level 1: exercise normal precautions', 
                2: 'Level 2: Exercise Increased Caution', 
                3: 'Level 3: Reconsider Travel', 
                4: 'Level 4: Do Not Travel'
                }.get(
                max(
                    int(re.search(r'\d+', PS_r1)),
                    int(re.search(r'\d+', PS_r2))
                ),
                ''
            )
            PS_df = pd.DataFrame(
                {
                    'pubDate':new_date, 
                    'ISO_A2':'PS', 
                    'Threat-Level': PS_rf
                }
            )
            df = pd.concat([df, PS_df], ignore_index=True)
            df.loc[df['ISO_A2']=='IL', 'Threat-Level'] = IL_r
        else:
            df
        break
    except Exception:
        if attempt < 4:
            time.sleep(5)
        else:
            raise
# %%
old_data = pd.read_csv(r'USSD_TAS.csv')
old_data['pubDate'] = pd.to_datetime(old_data.pubDate)
old_data.head()
filtered_data = pd.merge(df, old_data.groupby('ISO_A2')['pubDate'].max(), on='ISO_A2', how='left').sort_values(by=['ISO_A2'])
filtered_data.loc[~(filtered_data['pubDate_x'] ==filtered_data['pubDate_y'])]
filtered_data.drop('pubDate_y', axis=1, inplace=True)
filtered_data.rename(columns={'pubDate_x': 'pubDate'}, inplace=True)
# %%

current_data = pd.concat([filtered_data, old_data], ignore_index=True)
current_data.to_csv('USSD_TAS.csv', index=False, )
latest_df = current_data.sort_values(by=['ISO_A2', 'pubDate']).groupby('ISO_A2').last()

# %%
url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
world = gpd.read_file(url)
world.to_crs('+proj=wintri', inplace=True)
latest_gdf = world.merge(latest_df, left_on='ISO_A2_EH', right_on='ISO_A2', how='left') #due to natural world having diffrent standards than ISO alpha 2, the ISO_A2_EH column is recomended for this merger

# %%
today_t = today.strftime(r'%A, %B %d, %Y')
# %%
ax = latest_gdf.plot(
    column='Threat-Level', 
    edgecolor='black', 
    legend=True,
    cmap = 'viridis',
    missing_kwds= {
        'color': 'lightgrey',
        'hatch': '///',
        'label': 'No Data'
    },
    legend_kwds= {
        'loc': 'center left', 
        'bbox_to_anchor': (1, 0.5)
        }
    )
ax.set_axis_off()
ax.set_title(f'World Map of US State Department Travel Advisories for {today_t}', fontsize=14, fontweight='bold')

plt.savefig('travel_map.svg', bbox_inches='tight', dpi=600)
# %%
#https://geopandas.org/en/stable/docs/user_guide/interactive_mapping.html

