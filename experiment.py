import random
from typing import List, Tuple, Dict
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import re
from tqdm import tqdm
import pandas as pd

from constants import *
from contract import Contract, split_contract, read_file


def parse_filename(filename:str) -> Tuple[str, str]:
    parse_filename_pattern = re.compile(r"^(\d{2}-\w+)\.pdf_(\d+)$", re.IGNORECASE)  # IGNORECASE is critical since names might have both PDF and pdf
    match = parse_filename_pattern.search(filename)
    contract_number, tag = match.groups()
    identifier = f"{contract_number}_{tag}"
    return contract_number, tag, identifier


def check_lineprinter_table_files():
    filepaths_lineprinter = list(RAW_DATA_PATH_LINEPRINTER.glob('*.txt'))
    filepaths_table = list(RAW_DATA_PATH_TABLE.glob('*.txt'))
    assert [x.name for x in filepaths_lineprinter] == [x.name for x in filepaths_table]
    
    
def sort_contracts():
    """
    Goes through all the files and sorts them accordingly into 3 types. Saves contract types and other info to a CSV file.
    """
    check_lineprinter_table_files()

    filepaths_lineprinter = list(RAW_DATA_PATH_LINEPRINTER.glob('*.txt'))
    filepaths_doc = list(RAW_DATA_PATH_DOC.glob('*.txt'))

    filepaths = filepaths_lineprinter + filepaths_doc
    destination_path = PROCESSED_DATA_PATH

    print(f'Found {len(filepaths)} files.')
    print('Sorting by type and splitting multi-contract documents ...')
    
    destination_path.mkdir(exist_ok=True, parents=True) 

    contract_number_regex = re.compile(r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")

    contract_types = []
    
    cache = set()
    
    for filepath in tqdm(filepaths):
        row = defaultdict(str)
        
        filestem = filepath.stem
        row[FILENAME] = filestem
        
        tag = filestem.split('_')[-1]
        row[TAG] = tag
        
        file_contents = read_file(filepath)
        matches = re.findall(contract_number_regex, file_contents)

        if len(matches) == 1:
            # single contract files
            row[IDENTIFIER] = 't1_' + tag
            row[CONTRACT_TYPE] = 1
            if row[IDENTIFIER] in cache:
                print(f'Duplicated identifier: {row[IDENTIFIER]}.')
                continue
            cache.add(row[IDENTIFIER])
            if row[IDENTIFIER].strip() == '':
                print(f'Empty identifier: {row[IDENTIFIER]}.')

            shutil.copy(filepath.parent / filepath.name, destination_path / (row[IDENTIFIER] + '.txt'))
            contract_types.append(row)
            
        elif len(matches) > 1:
            # these are multiple contract files
            for key, new_file_contents in split_contract(file_contents, tag).items():
                identifier = 't1_' + key
                if identifier in cache:
                    print(f'Duplicated identifier: {identifier}.')
                    continue
                cache.add(identifier)
                
                new_row = row.copy()
                
                new_row[IDENTIFIER] = identifier        
                new_row[CONTRACT_TYPE] = 1
                
                if identifier.strip() == '':
                    print(f'Empty identifier: {row[IDENTIFIER]}.')
 
                with open(destination_path / (new_row[IDENTIFIER] + '.txt'), 'w') as output_file:
                    output_file.write(new_file_contents)
                
                contract_types.append(new_row)
        
        elif len(matches) == 0:
            # these are type2 contracts
            row[IDENTIFIER] = 't2_' + tag
            row[CONTRACT_TYPE] = 2
            
            if row[IDENTIFIER] in cache:
                print(f'Duplicated identifier: {row[IDENTIFIER]}.')
                continue
            
            if row[IDENTIFIER].strip() == '':
                print(f'Empty identifier: {row[IDENTIFIER]}.')
            shutil.copy(RAW_DATA_PATH_TABLE / filepath.name, destination_path / (row[IDENTIFIER] + '.txt'))
            contract_types.append(row)

    df = pd.DataFrame(contract_types)
    df.set_index('Filename', inplace=True)
    df.to_csv(RESULTS_PATH / 'contract_types.csv', index=True)
    
    print(f'Saved contracts to {destination_path}.')
    print('Generated results/contract_types.csv.')
    

def get_contract_types() -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Use as:
    d = get_contract_types()
    d['01-1234.pdf_1']  # return 1 or 2
    
    Note: The idea was is to have this dictionary for quick look up but I rarely use it as such.
    """
    df = pd.read_csv('data/contract_types.csv')
    df.set_index('Filename', inplace=True)
    return df, df['Contract_Type'].to_dict()


def get_contract_filepaths(contract_type: int, num_contracts=None, seed=42) -> List[Path]:
    """
    Gets num_contracts contracts of a specific type from folder. In theory, one can encode contract type into identifier, so this method could be eliminated.
    """
    if contract_type not in (1, 2):
        raise ValueError('contract_type must be 1 or 2.')
    filepaths = list(PROCESSED_DATA_PATH.glob(f't{contract_type}_*'))
    
    if seed:
        random.seed(seed)
    if num_contracts:
        filepaths = random.sample(filepaths, num_contracts)
    return filepaths


class Experiment:
    """
    Run extraction on contracts provided in filepaths.
    The results will be saved in a folder results using timestamp.
    """
    
    def __init__(self, filepaths: str | List[Path]):
        if isinstance(filepaths, str):
            self.filepaths = [Path(PROCESSED_DATA_PATH / (filepaths + '.txt'))]
        else:
            self.filepaths = filepaths
            
        self.timestamp = datetime.strftime(datetime.now(), '%m-%d-%Y-%H:%M:%S')
        self.make_results_path()
        
    def make_results_path(self):
        # Define result path for this specific experiment
        if len(self.filepaths) == 1:
            # 
            tag = self.filepaths[0].stem
            results_filename = f'{self.timestamp}:_{tag}'
        else:
            tag = f'{len(self.filepaths)}_contracts'
            
            # get contract_types
            contract_types = {filepath.stem[:2] for filepath in self.filepaths}
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
        
        # there is some overhead when appending to a DataFrame rather then creating a list and then converting to DataFrame, the only reason I don't annoying part is ffill 
        self.info = []
        self.bids = []
        self.subcontractors = []
        self.items = []
        self.errors = []
        
        n = len(self.filepaths)
        
        for i, filepath in enumerate(self.filepaths):
            contract_type = filepath.stem[:2]

            if i % 100 == 0:
                print(f"Processing {i+1}/{n} ... ")
            try:
                contract = Contract(filepath.stem)
                
                if len(self.filepaths) == 1:
                    self.contract = contract
                    
                contract.extract()
                
                self.info.extend(contract.info.rows)
                if not contract.postponed: 
                    self.subcontractors.extend(contract.subcontractors.rows)
                    self.bids.extend(contract.bids.rows)
                    self.items.extend(contract.items.rows)
                
            except Exception as e:
                print({CONTRACT_TYPE: contract_type, IDENTIFIER: filepath.stem, ERROR: e})
                self.errors.append([{IDENTIFIER: filepath.stem, ERROR: str(e), CONTRACT_TYPE: contract_type}])
                self.outliers_path.mkdir(exist_ok=True, parents=True)
                shutil.copy(filepath, self.outliers_path / filepath.name)
                
        print(f"Done processing {n} files.")
        
        self.write_to_disk()
                
    # def write_to_disk(self, df: pd.DataFrame | List, name: str):
    def write_to_disk(self):
        print("Writing to disk, please wait ...")
        
        # Create a Pandas Excel writer using openpyxl as the engine
        with pd.ExcelWriter(self.results_path / 'results.xlsx', engine='openpyxl') as writer:
            for obj, name in zip((self.info, self.bids, self.subcontractors, self.items, self.errors), ('Info', 'Bids', 'Subcontractors', 'Items', 'Errors')):
                obj = pd.DataFrame(obj)
                if obj.empty:
                    continue
                else:
                    print(f'Writing {name} ...')
                # Write the DataFrame to a new sheet in the Excel file using the file name as the sheet name
                obj.to_csv(self.results_path / f'{name}.csv', index=False)
                obj.to_excel(writer, sheet_name=name, index=False)
        print(f"Saved data to: {self.results_path}.")
