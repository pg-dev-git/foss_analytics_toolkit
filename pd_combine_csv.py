import pandas as pd
import glob
import os

extension = 'csv'
csv_files = glob.glob('*.{}'.format(extension))
combined_csv = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files])
combined_csv.to_csv( "AcquiredAccount_combined_csv.csv", index=False, encoding='utf-8-sig')
