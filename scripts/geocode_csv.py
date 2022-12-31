import argparse
from here import geocode_addresses
import csv

def geocode_csv(f, column):
    reader = csv.DictReader(f)
    addresses = (row[column] for row in reader)
    for place in geocode_addresses(addresses):
        if place:
            print(f"{place['position']['lat']},{place['position']['lng']}" )
        else:
            print('')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('r'))
    parser.add_argument('column', type=str)
    args = parser.parse_args()
    geocode_csv(args.file, args.column)
