import argparse

parser = argparse.ArgumentParser(description="Process source query args")
parser.add_argument("source", help="the name of the source")
args = parser.parse_args()

print(args.source + " abc")