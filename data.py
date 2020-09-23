"Get the latest cloud prices by either scraping or using the API."
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
        return time_since_mod < datetime.timedelta(seconds=7)

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

    remove = [
        "transferType",
        "fromLocation",
        "fromLocationType",
        "toLocation",
        "toLocationType",
        "instanceCapacity12xlarge",
        "instanceCapacity16xlarge",
        "instanceCapacity24xlarge",
        "instanceCapacity2xlarge",
        "instanceCapacity4xlarge",
        "instanceCapacity8xlarge",
        "instanceCapacityLarge",
        "instanceCapacityXlarge",
        "physicalCores",
        "group",
        "groupDescription",
        "resourceType",
        "provisioned",
        "volumeApiName",
        "ebsOptimized",
        "instanceCapacityMetal",
        "elasticGraphicsType",
        "gpuMemory",
        "instance",
        "instanceCapacity18xlarge",
        "instanceCapacity9xlarge",
        "instanceCapacity32xlarge",
        "productType",
        "storageMedia",
        "volumeType",
        "maxVolumeSize",
        "maxIopsvolume",
        "maxThroughputvolume",
        "instanceCapacityMedium",
        "maxIopsBurstPerformance",
        "instanceCapacity10xlarge",
        "servicecode",
        # "currentGeneration",
        "normalizationSizeFactor",
        "preInstalledSw",
        "processorFeatures",
        "servicename",
        "locationType",
        "enhancedNetworkingSupported",
        "instancesku",
        "processorArchitecture",
        "networkPerformance",
        "dedicatedEbsThroughput"
    ]

    def __init__(self, table_name='aws_data.pkl'):
        super().__init__(table_name)

    def setup(self):
        # Download latest pricing data
        data_name = 'ohio-ec2.json'
#         self.download_data(self.aws_pricing_index_ohio_url, data_name)

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

        products_df = pd.DataFrame(data).drop(columns=self.remove).set_index('sku')

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
                        'Unit': dim['unit'],
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
            gpu_name,gpu_ram = aws_gpu_ram[name[:2]] if float(count)>0 and name[:2] in aws_gpu_ram else ('',0)
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

#         os.remove(data_name)

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
            df = df.filter(['Name', 'CPUs', 'RAM (GB)']+(['GPUs', 'GPU RAM (GB)'] if gpus>0 else [])+['Price'])
        if not include_unk_price:
            df = df[df['Price ($/hr)'] != 0]

        df = df[(df['CPUs'] >= cpus) & (df['RAM (GB)'] >= ram)]
        if gpus > 0:
            df = df[(df['GPUs'] >= gpus) & (df['GPU RAM (GB)'] >= gpuram)]

        return df.sort_values('Price ($/hr)')[:n]
