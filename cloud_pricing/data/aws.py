import os
import pandas as pd
import json

from .core import FixedInstance


class AWSProcessor(FixedInstance):
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
            'gpu': 'GPUs', 'location': 'Region', 'storage': 'Storage'
        }, axis=1)

        # Change values to numbers
        combined['RAM (GB)'] = [float(a[:-4]) for a in combined['RAM (GB)'].values]
        combined[['CPUs','GPUs','Price ($/hr)','RAM (GB)']] = combined[['CPUs','GPUs','Price ($/hr)','RAM (GB)']].apply(pd.to_numeric)

        # Save data
        combined.to_pickle(self.table_name)

        os.remove(data_name)
