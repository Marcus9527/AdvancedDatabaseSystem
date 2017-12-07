import argparse
import transaction_manager

parser = argparse.ArgumentParser()
parser.add_argument('--inputdir', type=str, default='./input/', help='input file name')
parser.add_argument('--input', type=str, default='input', help='input file name')

args = parser.parse_args()

TM = transaction_manager.TransactionManager()
TM.parser(args.inputdir+args.input)
TM.print_final_status()
