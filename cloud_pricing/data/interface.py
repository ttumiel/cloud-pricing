"Get the latest cloud prices."
import requests
import math
from tqdm import tqdm
import pandas as pd
import os, re
import datetime, time
from pathlib import Path


class DataProcessor:
    "Process and store a table of data for a particular provider."
    def __init__(self, table_name):
        data_path = Path.home()/'.cloud-pricing-data'
        data_path.mkdir(exist_ok=True)
        self.float_re = re.compile(r'\d+\.\d+')
        self.int_re = re.compile(r'\d+')
        self.table_name = data_path/table_name
        if not self.has_setup:
            self.setup()
        self.table = pd.read_pickle(self.table_name)

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

    def extract_float(self, string):
        if isinstance(string, str):
            return self.float_re.search(string.replace(',', '')).group()
        elif isinstance(string, pd.Series):
            return string.apply(self.extract_float)
        else: return string

    def extract_int(self, string):
        if isinstance(string, str):
            return self.int_re.search(string.replace(',', '')).group()
        elif isinstance(string, pd.Series):
            return string.apply(self.extract_int)
        else: return string
class FixedInstance(DataProcessor):
    "Filter from a table of predefined instances"
    def filter(self, cpus, ram, gpus=0, gpuram=10, n=10, verbose=False, include_unk_price=False, spot=False):
        df = self.table.copy()
        price_name = 'Spot ($/hr)' if spot else 'Price ($/hr)'
        if not verbose:
            df = df.filter(['Name', 'CPUs', 'RAM (GB)']+(['GPUs', 'GPU RAM (GB)'] if gpus>0 else [])+[price_name])
        if not include_unk_price:
            df = df[(df[price_name] != 0) & (df[price_name] != float('nan'))]

        df = df[(df['CPUs'] >= cpus) & (df['RAM (GB)'] >= ram)]
        if gpus > 0:
            df = df[(df['GPUs'] >= gpus) & (df['GPU RAM (GB)'] >= gpuram)]

        return df.sort_values(price_name)[:n]

class CustomInstance(DataProcessor):
    """Process instances that can be customized on demand
    by selecting the cpus, gpus, etc. and multiplying by
    the cost per unit.

    Should setup self.cpu_pricing and self.gpu_pricing
    """
    def filter(self, cpus, ram, gpus=0, gpuram=10, verbose=False):
        if ram//cpus > 8:
            print("More than 8GB of RAM per CPU may lead to additional costs.")

        # Handle CPUs and RAM
        df = self.cpu_pricing.copy()
        df['CPU Price ($/hr)'] = df['Price ($/hr)'] * cpus
        df['RAM ($/hr)'] = df['RAM ($/hr)'] * ram
        df.insert(0, 'CPUs', [cpus]*len(df))
        df.insert(0, 'RAM (GB)', [ram]*len(df))
        df['Price ($/hr)'] = df['CPU Price ($/hr)'] + df['RAM ($/hr)']

        if gpus>0:
            # Handle GPUs and GPU RAM
            gpus_df = self.gpu_pricing.copy()
            counts = [min(max(gpus, math.ceil(gpuram/r)),m)
                     for r,m in zip(gpus_df['GPU RAM (GB)'], gpus_df['Max #'])]
            gpus_df.insert(0, 'GPUs', counts)
            gpus_df['GPU Price ($/hr)'] = gpus_df['GPU Price ($/hr)'] * counts
            gpus_df['GPU RAM (GB)'] = gpus_df['GPU RAM (GB)'] * counts

            # For every CPU/RAM combination, attach the required GPUs
            df = pd.concat([
                pd.DataFrame([
                    pd.concat([df.iloc[i], gpus_df.iloc[j]])
                    for i in range(len(df))
                ], columns=df.columns.append(gpus_df.columns))
                for j in range(len(gpus_df))
            ])

            # Update total price
            df['Price ($/hr)'] = df['Price ($/hr)'] + df['GPU Price ($/hr)']

        return df.filter(['Name', 'Type', 'CPUs', 'RAM (GB)']+
                         (['GPUs', 'GPU RAM (GB)'] if gpus>0 else [])+['Price ($/hr)']).sort_values('Price ($/hr)')
