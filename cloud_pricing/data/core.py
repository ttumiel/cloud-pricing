import pandas as pd

from cloud_pricing import data

PROVIDERS = {
    'GCP': data.GCPProcessor,
    'AWS': data.AWSProcessor,
    'AZURE': data.AzureProcessor,
}


class CloudProcessor:
    def __init__(self, providers="ALL"):
        self._tables = []

        if providers == 'ALL':
            processors = list(PROVIDERS.values())
        else:
            processors = [PROVIDERS[p] for p in providers.split(',')]

        for t in processors:
            self._tables.append(t())

    def update(self):
        for t in self._tables:
            t.setup()

    # TODO: Add prefix to all labels that are in only one of the processors (like aws-)
    # Clean up the args here
    def filter(self, cpus, ram, gpus=0, gpuram=10, n=10, verbose=False, include_unk_price=False, spot=False):
        price_name = 'Spot ($/hr)' if spot else 'Price ($/hr)'
        return pd.concat([t.filter(cpus, ram, gpus, gpuram, n=-1, verbose=verbose, include_unk_price=include_unk_price, spot=spot) for t in self._tables], sort=False).sort_values(price_name)[:n]
