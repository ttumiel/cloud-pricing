"Main CLI"

import argparse

from data import CloudProcessor

def main():
    parser = argparse.ArgumentParser(description="Compare cloud pricing on the command line. Set the required compute and receive a table of compatible prices. For some services (like AWS) the instance type reflects the best fit given the input constraints.")
    parser.add_argument("--cpus", "-c", default=4, type=int,
        help="Number of CPUs to request.")
    parser.add_argument("--gpus", "-g", default=0, type=int,
        help="Number of GPUs to request.")
    parser.add_argument("--ram", "-r", default=8, type=int,
        help="Amount of RAM in Gb.")
    parser.add_argument("--gpuram", "--gr", default=10, type=int,
        help="Amount of total GPU RAM in Gb.")
    parser.add_argument("--verbose", "-v", default=False, action='store_true',
        help="Increase verbosity, showing all info columns.")
    parser.add_argument("-n", default=10, type=int,
        help="The number of results to show.")
    parser.add_argument("--unk_price", "-U", default=False, action='store_true',
        help="Exclude products that don't have a known price.")
    parser.add_argument("--out", "-o", default=None, type=str,
        help="Save the outputs to a file. {csv | json}")

    args = parser.parse_args()
    print(args)
    proc = CloudProcessor()
    data = proc.filter(args.cpus, args.ram, args.gpus, args.gpuram, args.n, args.verbose, args.unk_price)

    if args.out is not None:
        if args.out.endswith('csv'):
            data.to_csv(args.out)
        elif args.out.endswith('json'):
            data.to_json(args.out)
    else:
        print(data)

if __name__ == "__main__":
    main()
