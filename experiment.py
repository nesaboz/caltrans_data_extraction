from typing import Dict
import random
from constants import *
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
from tqdm import tqdm
import pandas as pd
from enum import Enum
from typing import List, Tuple

from contract import Contract

from utils import parse_filename, read_file
from contract import Contract, split_contract
from contract_type2 import Contract2


def sort_contracts():
    """
    Goes through all the files and sorts them accordingly into 3 types. Saves contract types and other info to a CSV file.
    """
    
    filepaths_lineprinter = list(RAW_DATA_PATH_LINEPRINTER.glob('*.txt'))
    filepaths_table = list(RAW_DATA_PATH_TABLE.glob('*.txt'))
    assert [x.name for x in filepaths_lineprinter] == [x.name for x in filepaths_table]

    print(f'Found {len(filepaths_lineprinter)} files in lineprinter/table folder. Started sorting ...')

    contract_number_regex = re.compile(r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")

    TYPE1_PATH.mkdir(exist_ok=True, parents=True)
    TYPE2_PATH.mkdir(exist_ok=True, parents=True)
    TYPE3_PATH.mkdir(exist_ok=True, parents=True)

    contract_types = []

    for filepath in tqdm(filepaths_lineprinter):
        row = defaultdict(str)
        
        filestem = filepath.stem
        row[FILENAME] = filestem
        row[CONTRACT_NUMBER], tag, identifier = parse_filename(filestem)

        file_contents = read_file(filepath)
        matches = re.findall(contract_number_regex, file_contents) 
        
        row[TAG] = tag
        row[IDENTIFIER] = identifier
        
        if len(matches) == 1:
            row[CONTRACT_TYPE] = ContractType.TYPE1.value
            row[RELATIVE_PATH] = (TYPE1_PATH / (identifier + '.txt')).relative_to(RAW_DATA_PATH)
            shutil.copy(RAW_DATA_PATH_LINEPRINTER / filepath.name, RAW_DATA_PATH / row[RELATIVE_PATH])
            contract_types.append(row)
            
        elif len(matches) == 0:
            row[CONTRACT_TYPE] = ContractType.TYPE2.value
            row[RELATIVE_PATH] = (TYPE2_PATH / (identifier + '.txt')).relative_to(RAW_DATA_PATH)
            shutil.copy(RAW_DATA_PATH_TABLE / filepath.name, RAW_DATA_PATH / row[RELATIVE_PATH])
            contract_types.append(row)
            
        elif len(matches) > 1:
            row[CONTRACT_TYPE] = ContractType.TYPE3.value
            try:
                # here we need to split the file into multiple contracts
                for new_identifier, new_file_contents in split_contract(identifier, file_contents, tag):
                    new_row = row.copy()
                    new_row[IDENTIFIER] = new_identifier
                    new_row[ORIGINAL_IDENTIFIER] = identifier
                    
                    new_path = TYPE3_PATH / (new_identifier + '.txt')
                    new_row[RELATIVE_PATH] = new_path.relative_to(RAW_DATA_PATH)
                    
                    with open(new_path, 'w') as output_file:
                        output_file.write(new_file_contents)
                    
                    contract_types.append(new_row)
            except Exception as e:
                print(f'Error processing {identifier}: {e}')
            

    df = pd.DataFrame(contract_types)
    df.set_index('Filename', inplace=True)
    df.to_csv(RAW_DATA_PATH / 'contract_types.csv', index=True)
    
    
    l = [len(list(x.glob('*'))) for x in [TYPE1_PATH, TYPE2_PATH, TYPE3_PATH]]
    print(f'Saved {l[0]} contracts to type1 folder')
    print(f'Saved {l[1]} contracts to type2 folder')
    print(f'Saved {l[2]} contracts to type3 folder')
    

def get_contract_types() -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Use as:
    d = get_contract_types()
    d['01-1234.pdf_1']  # return 1 or 2
    """
    df = pd.read_csv('data/contract_types.csv')
    df.set_index('Filename', inplace=True)
    return df, df['Contract_Type'].to_dict()


def get_contract_filepaths(contract_type: ContractType, num_contracts=None, seed=42):
    df_contract_types, _ = get_contract_types()
    files = list(df_contract_types[df_contract_types[CONTRACT_TYPE] == contract_type.value][RELATIVE_PATH].values)
    if seed:
        random.seed(seed)
    if num_contracts:
        files = random.sample(files, num_contracts)
    filepaths = [RAW_DATA_PATH / x for x in files]
    return filepaths


class Experiment:
    """
    Run extraction on many contracts provided in filepaths.
    
    The results folder will be determined automatically.
    """
    
    def __init__(self, filepaths: str | List[Path]):
        if isinstance(filepaths, str):
            self.filepaths = [Path(RAW_DATA_PATH / (filepaths + '.txt'))]
        else:
            self.filepaths = filepaths
            
        self.timestamp = datetime.strftime(datetime.now(), '%m-%d-%Y-%H:%M:%S')
        self.make_results_path()
        
    def make_results_path(self):
        # Define result path for this specific experiment
        if len(self.filepaths) == 1:
            tag = self.filepaths[0].stem
            contract_type = self.filepaths[0].parent.stem
        else:
            tag = f'{len(self.filepaths)}_contracts'
            
            # get contract_types
            contract_types = {filepath.parent.stem for filepath in self.filepaths}
            if len(contract_types) == 1:
                contract_type = contract_types.pop()
            else:
                contract_type = 'type_mixed'
                
        results_filename = f'{self.timestamp}:_{tag}_{contract_type}'
        self.results_path = RESULTS_PATH / results_filename
        self.outliers_path = self.results_path / 'outliers'
        
        # Create the results folders
        self.results_path.mkdir(exist_ok=True, parents=True)

    
    def run(self):
        """
        Run a batch or a single file (by making `files` a single element list).
        """
        
        self.df_info = pd.DataFrame()
        self.df_bids = pd.DataFrame()
        self.df_subcontractors = pd.DataFrame()
        self.df_items = pd.DataFrame()
        self.df_errors = pd.DataFrame()
        
        n = len(self.filepaths)
        
        for i, filepath in enumerate(self.filepaths):
            contract_type = filepath.parent.stem

            if i % 100 == 0:
                print(f"Processing {i+1}/{n} ... ")
            try:
                if contract_type == 'type1':
                    contract = Contract(os.path.join(contract_type, filepath.stem))
                elif contract_type == 'type2':
                    contract = Contract2(os.path.join(contract_type, filepath.stem))
                elif contract_type == 'type3':
                    contract = Contract(os.path.join(contract_type, filepath.stem))
                else:
                    raise ValueError(f"Unknown contract type: {contract_type}")
                    
                if len(self.filepaths) == 1:
                    self.contract = contract
                    
                contract.extract()
                
                self.df_info = pd.concat([self.df_info, contract.info.df])
                self.df_bids = pd.concat([self.df_bids, contract.bids.df])
                self.df_subcontractors = pd.concat([self.df_subcontractors, contract.subcontractors.df])
                self.df_items = pd.concat([self.df_items, contract.items.df])
                
            except Exception as e:
                print({CONTRACT_TYPE: contract_type, IDENTIFIER: filepath.stem, ERROR: e})
                self.df_errors = pd.concat([self.df_errors, pd.DataFrame([{IDENTIFIER: filepath.stem, ERROR: str(e), CONTRACT_TYPE: contract_type}])])
                
                self.outliers_path.mkdir(exist_ok=True, parents=True)
                shutil.copy(filepath, self.outliers_path / filepath.name)
        print(f"Done processing {n} files.")
        
        self.write_to_disk()
                
    # def write_to_disk(self, df: pd.DataFrame | List, name: str):
    def write_to_disk(self):
        print("Writing to disk ...")
        
        # Create a Pandas Excel writer using openpyxl as the engine
        with pd.ExcelWriter(self.results_path / 'results.xlsx', engine='openpyxl') as writer:
            for obj, name in zip((self.df_info, self.df_bids, self.df_subcontractors, self.df_items, self.df_errors), ('Info', 'Bids', 'Subcontractors', 'Items', 'Errors')):
                if obj.empty:
                    continue
                # Write the DataFrame to a new sheet in the Excel file using the file name as the sheet name
                obj.to_csv(self.results_path / f'{name}.csv', index=False)
                obj.to_excel(writer, sheet_name=name, index=False)
                
        print(f"Saved data to: {self.results_path}.")
