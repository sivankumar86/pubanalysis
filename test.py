
import argparse, sys

parser=argparse.ArgumentParser()
parser.add_argument('--input', help='pass your s3 input path')
parser.add_argument('--output', help='pass your s3 output path')
args=parser.parse_args()
print(args.input)
