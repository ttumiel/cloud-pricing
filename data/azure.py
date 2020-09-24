import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import numpy as np

from core import FixedInstance


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
