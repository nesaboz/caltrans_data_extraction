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
    
    Note: The idea was is to have this dictionary for quick look up but I rarely use it as such.
    """
    df = pd.read_csv('data/contract_types.csv')
    df.set_index('Filename', inplace=True)
    return df, df['Contract_Type'].to_dict()


def get_contract_filepaths(contract_type: ContractType, num_contracts=None, seed=42) -> List[Path]:
    """
    Gets num_contracts contracts of a specific type from folder. In theory, one can encode contract type into identifier, so this method could be eliminated.
    """
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
    Run extraction on contracts provided in filepaths.
    The results will be saved in a folder results using timestamp.
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
        
        # there is some overhead when appending to a DataFrame rather then creating a list and then converting to DataFrame, the only reason I don't annoying part is ffill 
        self.info = []
        self.bids = []
        self.subcontractors = []
        self.items = []
        self.errors = []
        
        n = len(self.filepaths)
        
        for i, filepath in enumerate(self.filepaths):
            contract_type = filepath.parent.stem

            if i % 100 == 0:
                print(f"Processing {i+1}/{n} ... ")
            try:
                contract = Contract(os.path.join(contract_type, filepath.stem))
                
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
                # Write the DataFrame to a new sheet in the Excel file using the file name as the sheet name
                obj.to_csv(self.results_path / f'{name}.csv', index=False)
                obj.to_excel(writer, sheet_name=name, index=False)
                print(f'Wrote {name}.')
        print(f"Saved data to: {self.results_path}.")
