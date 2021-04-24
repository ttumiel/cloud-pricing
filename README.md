# Cloud Pricing CLI

View and compare cloud prices for a particular kind of instance on demand. Simply provide the specs for the instance and you'll receive the results of the query in the console.


## Try it

```bash
# Quick install. See below for editable install.
pip install git+https://github.com/ttumiel/cloud-pricing

# Query all prices that have at least 8 cpus, 32GB RAM,
# 1 GPU with 16GB of GPU RAM
cloud-pricing --cpus 8 --ram 32 --gpus 1 --gpuram 16

# For more info about the flags, see help
cloud-pricing -h
```

## Usage

```sh
usage: cloud-pricing [-h] [--cpus CPUS] [--gpus GPUS] [--ram RAM]
                     [--gpuram GPURAM] [--verbose] [-n N] [--unk_price]
                     [--out OUT] [--spot] [--update] [--providers PROVIDERS]

Compare cloud pricing on the command line. Set the required compute and
receive a table of compatible prices. For some services (like AWS) the
instance type reflects the best fit given the input constraints.

optional arguments:
  -h, --help            show this help message and exit
  --cpus CPUS, -c CPUS  Number of CPUs to request.
  --gpus GPUS, -g GPUS  Number of GPUs to request.
  --ram RAM, -r RAM     Amount of RAM in Gb.
  --gpuram GPURAM, --gr GPURAM
                        Amount of total GPU RAM in Gb.
  --verbose, -v         Increase verbosity, showing all info columns.
  -n N                  The number of results to show.
  --unk_price, -P       Exclude products that don't have a known price.
  --out OUT, -o OUT     Save the outputs to a file. {csv | json}
  --spot, -s            Use spot (preemptible) prices.
  --update, -U          Force an update to the database of prices.
  --providers PROVIDERS
                        List of providers to search over. Comma separated
                        string of 'AWS', 'Azure', 'GCP', or 'All'. Example:
                        'aws,gcp'

```

## Development

Currently, the program downloads the latest data from various cloud providers (AWS, Azure, GCP, at this point), parses and saves it into a pandas DataFrame and then displays the results on querying. Azure and GCP don't seem to provide any formal datasheet for prices so the data is scraped directly from their respective sites. If you know of a better way to get the data, please let me know. Additionally, I haven't fully checked all of the respective data sources, and these prices are not always correct. The AWS offer sheet also seems to show some prices that I can't seem to find on the console.

If you have any ideas for these problems, or just want to contribute, let me know!

To install the package as an editable installation:

```bash
git clone https://github.com/ttumiel/cloud-pricing
cd cloud-pricing
pip install -e .
```

Some things I'd like to add:
- Spot instances
- Storage costs
