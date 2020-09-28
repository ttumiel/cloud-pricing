import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

from data.core import FixedInstance


class GCPProcessor(FixedInstance):
    """Process data from Google Cloud.

    This processor scrapes the pricing data from the compute
    pricing page. Google has both a set of predefined instances
    as well as customisable instances - both of which are handled.
    """
    url ='https://cloud.google.com/compute/all-pricing'
    gpu_instances = ['n1']
    gcloud_region_shortcodes = {
        'us-central1': 'io','us-west1': 'ore','us-west2': 'la',
        'us-west3': 'slc','us-west4': 'lv','us-east4': 'nv',
        'us-east1': 'sc','northamerica-northeast1': 'mtreal',
        'southamerica-east1': 'spaulo','europe-west1': 'eu',
        'europe-north1': 'fi','europe-west2': 'lon',
        'europe-west3': 'ffurt','europe-west4': 'nether',
        'europe-west6': 'zur','asia-south1': 'mbai',
        'asia-southeast1': 'sg','asia-southeast2': 'jk',
        'australia-southeast1': 'syd','asia-east1': 'tw',
        'asia-east2': 'hk','asia-northeast1': 'ja',
        'asia-northeast2': 'osa','asia-northeast3': 'kr'
    }

    def __init__(self, table_name='gcp_data.pkl'):
        super().__init__(table_name)

    def combine_custom_df(self, df):
        "Clean and rename custom dfs"
        if 'On-demand price' not in df:
            return None

        same_cols = ['Region', 'Item', 'Name']
        out = df.filter(['Region', 'Name'])[:1]

        # Combine the CPU and Memory data into a single row
        cpus = df[df['Item'].str.contains('CPU')].drop(columns=same_cols).add_suffix(' (CPU)')
        if len(cpus) > 0:
            for c in cpus.columns:
                out[c] = cpus[c].values

        ram = df[df['Item'].str.contains('memory', case=False)].drop(columns=same_cols).add_suffix(' (RAM)')
        if len(ram) > 0:
            for c in ram.columns:
                out[c] = ram[c].values

        out = out[[a!='Not available in this region' for a in out['On-demand price (CPU)']]]
        out = out.rename(columns={
            'On-demand price (CPU)': 'CPU Price ($/hr)',
            'On-demand price (RAM)': 'RAM ($/hr)'
        })

        return out

    def combine_predefined_df(self, df):
        "Clean and rename predefined dfs"
        df = df.rename(columns={'Evaluative price': 'Price'})
        if 'Machine type' not in df or 'Price' not in df:
            return

        df = df[(df['Price'] != 'Not available in this region') & (df['Price'].values != None)]
        df = df.drop(columns='Name')
        df['Price'] = df['Price'].apply(lambda x: re.search(r'\d+\.\d+', x).group())
        df['Memory'] = df['Memory'].apply(lambda x: re.search(r'\d+', x).group())
        df = df.rename(columns={
            'Virtual CPUs': 'CPUs',
            'vCPUs': 'CPUs',
            'Price': 'Price ($/hr)',
            'Memory': 'RAM (GB)',
            'Machine type': 'Name'
        })

        return df

    def extract_table(self, table, region='us-east1'):
        rows = table.find_all('tr')
        titles = None
        all_data = []
        spans = []
        rowspan = 0
        for i,row in enumerate(rows):
            if titles is None:
                heads = row.find_all('th')
                assert len(heads) > 0, "Oops, Missing Header!"
                titles = [h.get_text().strip().replace(' (USD)', '') for h in heads]

            # If a row repeats then save the items as a list in the output
            if len(spans) > 0 and rowspan > 0:
                data = row.find_all('td')
                rowspan -= 1
                assert len(spans) == len(data)
                for d,s in zip(data, spans):
                    if not isinstance(all_data[-1][s], list):
                        all_data[-1][s] = [all_data[-1][s]]
                    all_data[-1][s].append(d.get_text().strip())
            else:
                spans = []
                row_data = []
                for j,d in enumerate(row.find_all('td')[:len(titles)]):
                    if rowspan>0 and not d.has_attr('rowspan'):
                        spans.append(j)
                    elif d.has_attr('rowspan'):
                        rowspan = int(d.get('rowspan'))-1

                    row_data.append(d.get_text().strip())
                    if d.has_attr(self.gcloud_region_shortcodes[region]+'-hourly'):
                        row_data[-1] = d.get(self.gcloud_region_shortcodes[region]+'-hourly')
                    if row_data[-1] == '' and d.has_attr('default'): row_data[-1] = d.get('default').strip()

                if len(row_data) > 0:
                    all_data.append(row_data)

        df = pd.DataFrame(all_data, columns=titles)
        df.insert(0, 'Region', region)
        return df

    def get_table(self, frame):
        "Scrape and extract a table from the GCloud website."
        data = requests.get('https://cloud.google.com'+frame)
        soup = BeautifulSoup(data.content, 'lxml')
        return self.extract_table(soup.find('table'))

    def setup(self):
        """The GCP cloud pricing site places the pricing tables of each
        instance type into a separate iframe. The iframes themselves don't
        always contain the name of the instance so we have to keep track
        of that from the main pricing page. We go through the page storing
        the titles and getting the tables, appending each to our list
        of dataframes.
        """
        print('Setting up GCP data...')
        r = requests.get(self.url)
        s = BeautifulSoup(r.content, 'lxml')
        pricing_body = s.find(class_='devsite-article-body')

        custom = False
        dfs = []
        gpu_dfs = []
        for i in pricing_body:
            if i.name is not None:
                if i.name.lower() in {'h2', 'h3', 'h4'}:
                    current_name = i.get('data-text')
                t = i.find('iframe')
                if t is not None:
                    print(current_name)

                    # Always add GPU tables
                    if 'GPU' in current_name:
                        df = self.get_table(t.get('src'))
                        df.insert(0, 'Name', current_name)
                        gpu_dfs.append(df)

                    # Look for custom tables if custom else predefined tables
                    elif not(custom ^ ('custom' in current_name)):
                        # Scrape table
                        df = self.get_table(t.get('src'))
                        df.insert(0, 'Name', current_name)

                        # Process custom instance tables
                        if custom: df = self.combine_custom_df(df)
                        # Process predefined instance tables
                        else: df = self.combine_predefined_df(df)

                        dfs.append(df)

        # Concat all the tables into 1
        df = pd.concat(dfs, sort=False).reset_index(drop=True)

        # Turn all number columns into numbers and remove "not available" text
        df = df.apply(lambda x: [(re.search(r'\d+\.\d+', q)[0] if isinstance(q, str) and q.startswith('$') else q) for q in x.values])
        df = df.replace({
            'Not available in this region': float('nan')
        })
        numerics=list(set(df.columns)-{'Name','Region'})
        df[numerics] = df[numerics].apply(pd.to_numeric)

        # Process GPU Table
        gpus = pd.concat(gpu_dfs)
        gpus = gpus[(gpus['GPU price'] != 'Not available in this region') & (gpus['GPU price'].values != None)]
        gpus['GPUs'] = [[re.search('\d+', v).group() for v in mem] for mem in gpus['GPUs']]
        gpus['GPU memory'] = [min(float(re.search(r'\d+', v).group()) for v in mem) for mem in gpus['GPU memory']]
        gpus = gpus.drop(columns=['Name'])

        # Extract floats from all dollar amounts
        gpus['GPU price'] = gpus['GPU price'].apply(lambda x: re.search(r'\d+\.\d+', x).group())

        # Rename columns
        gpus = gpus.rename(columns={
            'GPUs': 'GPU Counts',
            'GPU price': 'Price ($/hr)',
            'GPU memory': 'RAM (GB)',
            'Model': 'Name'
        })

        # Make numeric
        gpus[['Price ($/hr)', 'RAM (GB)']] = gpus[['Price ($/hr)', 'RAM (GB)']].apply(pd.to_numeric)

        # Make predefined GPU instances for the instances that can use GPUs
        if not custom:
            gpu_dfs = []

            # Create a new table of GPUs & instances for every GPU type
            # and for every amount of GPUs per instance.
            g=gpus.explode('GPU Counts')
            g=g.rename(columns={'GPU Counts': 'GPUs'})
            g['GPUs'] = g['GPUs'].apply(pd.to_numeric)
            g['RAM (GB)'] = g['RAM (GB)'] * g['GPUs']
            g['Price ($/hr)'] = g['Price ($/hr)'] * g['GPUs']

            for gi in self.gpu_instances:
                gi_df = df[df['Name'].str.startswith(gi)]
                g.columns = [('GPU '+c if c in set(gi_df.columns) else c) for c in g.columns]
                out = pd.concat([
                        pd.DataFrame([pd.concat([gi_df.iloc[j], g.iloc[i]]) for i in range(len(g))])
                        for j in range(len(gi_df))
                        ])
                out['Name'] = out['Name'] + ' with GPU'
                out['Price ($/hr)'] = out['Price ($/hr)'] + out['GPU Price ($/hr)']
                gpu_dfs.append(out)

            table = pd.concat([df, pd.concat(gpu_dfs, sort=False).reset_index(drop=True)],
                                   ignore_index=True, sort=False)
            table.to_pickle(self.table_name)
        else:
            self.cpu_pricing = df
            self.gpu_pricing = gpus
