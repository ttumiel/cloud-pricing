"Get the latest cloud prices."
import requests
import math
from tqdm import tqdm
import json
import pandas as pd
import os
import datetime, time


class DataProcessor:
    def __init__(self, table_name):
        self.table_name = table_name
        if not self.has_setup:
            self.setup()
        self.table = pd.read_pickle(table_name)

    def setup(self):
        raise NotImplementedError

    @property
    def has_setup(self):
        if not os.path.exists(self.table_name): return False
        mod_time = os.path.getmtime(self.table_name)
        time_since_mod = datetime.timedelta(seconds=time.time()-mod_time)
        return time_since_mod < datetime.timedelta(days=7)

    def __repr__(self):
        return repr(self.table)

    def download_data(self, url, fileout):
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            l = math.ceil(float(r.headers['Content-Length'])/8192)
            with open(fileout, 'wb') as f:
                for chunk in tqdm(r.iter_content(chunk_size=8192), total=l):
                    f.write(chunk)

    def filter(self, cpus, ram, gpus=0, gpuram=10, n=10, verbose=False, include_unk_price=False):
        df = self.table.copy()
        if not verbose:
            df = df.filter(['Name', 'CPUs', 'RAM (GB)']+(['GPUs', 'GPU RAM (GB)'] if gpus>0 else [])+['Price ($/hr)'])
        if not include_unk_price:
            df = df[df['Price ($/hr)'] != 0]

        df = df[(df['CPUs'] >= cpus) & (df['RAM (GB)'] >= ram)]
        if gpus > 0:
            df = df[(df['GPUs'] >= gpus) & (df['GPU RAM (GB)'] >= gpuram)]

        return df.sort_values('Price ($/hr)')[:n]


class AWSProcessor(DataProcessor):
    aws_gpu_ram = {
        'p3': ('V100', 16),
        'p2': ('K80', 12),
        'g4': ('T4', 16),
        'g3': ('M60', 8)
    }

    aws_pricing_index_ohio_url = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-east-1/index.json"

    include_cols = [
        'instanceType', 'location', 'productFamily',
        'instanceFamily', 'currentGeneration',
        'physicalProcessor', 'clockSpeed', 'sku',
        'storage', 'tenancy', 'operatingSystem',
        'capacitystatus', 'vcpu', 'memory', 'gpu'
    ]

    def __init__(self, table_name='aws_data.pkl'):
        super().__init__(table_name)

    def setup(self):
        print("Downloading latest AWS data...")

        # Download latest pricing data
        data_name = 'ohio-ec2.json'
        self.download_data(self.aws_pricing_index_ohio_url, data_name)

        with open('ohio-ec2.json', 'r') as f:
            raw_aws_data=json.load(f)

        # Create products table
        data = []
        for p in raw_aws_data['products'].values():
            data.append({
                'sku': p['sku'],
                'productFamily': p['productFamily'],
                **p['attributes']
            })

        products_df = pd.DataFrame(data).filter(self.include_cols).set_index('sku')

        # Create pricing table
        on_demand = raw_aws_data['terms']['OnDemand']
        pricing_data = []
        all_skus = set()
        for sku,v in on_demand.items():
            for offer in v.values():
                for dim in offer['priceDimensions'].values():
                    if sku in all_skus: print("Duplicate SKU", sku)
                    else: all_skus.add(sku)
                    pricing_data.append({
                        'sku': sku,
                        'Price ($/hr)': dim['pricePerUnit']['USD'] if 'USD' in dim['pricePerUnit'] else dim['pricePerUnit']
                    })

        pricing_df = pd.DataFrame(pricing_data).set_index('sku')

        # Join products and prices and filter to compute instances
        combined = products_df.join(pricing_df)
        combined = combined[combined['productFamily'] ==  'Compute Instance']
        combined = combined.drop(columns=['productFamily'])

        # Generate GPU RAM and names from instance names
        gpu_names,gpu_rams = [],[]
        for name,count in zip(combined['instanceType'].values, combined['gpu'].values):
            gpu_name,gpu_ram = self.aws_gpu_ram[name[:2]] if float(count)>0 and name[:2] in self.aws_gpu_ram else ('',0)
            gpu_names.append(gpu_name)
            gpu_rams.append(gpu_ram*float(count) if float(count)>0 else 0)
        combined.insert(len(combined.columns)-2, 'GPU Name', gpu_names)
        combined.insert(len(combined.columns)-2, 'GPU RAM (GB)', gpu_rams)

        # Rename columns
        combined = combined.rename({
            'vcpu': 'CPUs', 'memory': 'RAM (GB)', 'instanceType': 'Name',
            'gpu': 'GPUs', 'location': 'Region'
        }, axis=1)

        # Change values to numbers
        combined['RAM (GB)'] = [float(a[:-4]) for a in combined['RAM (GB)'].values]
        combined[['CPUs','GPUs','Price ($/hr)','RAM (GB)']] = combined[['CPUs','GPUs','Price ($/hr)','RAM (GB)']].apply(pd.to_numeric)

        # Save data
        combined.to_pickle(self.table_name)

class AzureProcessor(FixedInstance):
    url = 'https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/'
    azure_gpus_ram = {
        'K80': 12, 'M60': 8, 'P100': 16, 'P40': 24,
        'T4': 16, 'V100': 16, np.nan: 0
    }

    def __init__(self, table_name='azure_data.pkl'):
        super().__init__(table_name)

    def extract_table(self, table, region='us-east'):
        rows = table.find_all('tr')
        titles = None
        all_data = []
        for row in rows:
            if titles is None:
                heads = row.find_all('th')
                assert len(heads) > 0, "Oops, Missing Header!"
                titles = [h.get_text().strip() for h in heads]

            row_data = []
            for d in row.find_all('td')[:len(titles)]:
                row_data.append(d.get_text().strip())
                if d.find_next().has_attr('data-amount'):
                    row_data[-1] = json.loads(d.find_next().get('data-amount'))['regional'].get(region, None)

            if len(row_data) > 0:
                all_data.append(row_data)

        return pd.DataFrame(all_data, columns=titles)

    def download_data(self):
        f = requests.get(self.url)
        soup = BeautifulSoup(f.content)
        self.tables = soup.find_all('table')

    def setup(self):
        self.download_data()

        # Extract each table and pricing data from HTML
        dfs = [self.extract_table(t) for t in tables if len(t.find_all('th')) > 0]

        # Parse, clean and combine data
        dfs = [df for df in dfs if any(c in df.columns for c in {'vCPU(s)', 'GPU', 'Core', 'RAM'})]
        cat = pd.concat(dfs, sort=False)
        cat['vCPU(s)'] = [(v if v is not np.nan else c) for v,c in zip(cat['vCPU(s)'], cat['Core'])]
        cat = cat.filter(['Instance', 'vCPU(s)', 'RAM', 'GPU', 'Pay as you go']).rename({
            'vCPU(s)': 'CPUs',
            'RAM': 'RAM (GB)',
            'Pay as you go': 'Price ($/hr)',
            'GPU': 'GPUs',
            'Instance': 'Name'
        }, axis=1)
        cat = cat.replace({'– –\nBlank': np.nan, 'N/A': np.nan}, regex=True).reset_index(drop=True)

        # Parse GPU info
        n_gpus, gpu_names = [],[]
        for g in cat['GPUs'].values:
            if isinstance(g, str):
                n,t = g.split()[:2]
                n_gpus.append(int(n[:-1]))
                gpu_names.append(t)
            else:
                n_gpus.append(np.nan)
                gpu_names.append(np.nan)

        n_gpus = np.array(n_gpus)
        gpu_ram = np.array([self.azure_gpus_ram[gpu_name] for gpu_name in gpu_names])
        gpu_ram = n_gpus*gpu_ram

        cat['GPUs'] = n_gpus
        cat.insert(len(cat.columns)-2, 'GPU Name', gpu_names)
        cat.insert(len(cat.columns)-2, 'GPU RAM (GB)', gpu_ram)

        # Convert numbers
        cat['RAM (GB)'] = [(float(a[:-4].replace(',', '')) if isinstance(a, str) else 0.) for a in cat['RAM (GB)'].values]
        cat[['CPUs','GPUs','Price ($/hr)','RAM (GB)']] = cat[['CPUs','GPUs','Price ($/hr)','RAM (GB)']].apply(pd.to_numeric)

        cat.to_pickle(self.table_name)
