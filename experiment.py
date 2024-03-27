from typing import Dict
from contract import *
import random
    

def save_contract_types():
    """
    Inspect quickly contract to determine if it is of type 1 or 2.
    """
    filepaths = RAW_DATA_PATH_LINEPRINTER.glob('*.txt')

    contract_types = []
    for i, filepath in enumerate(filepaths):
        row = defaultdict(str)
        
        row[RELATIVE_PATH] = filepath.relative_to(RAW_DATA_PATH)
        filename = filepath.stem
        row[FILENAME] = filename
        row[CONTRACT_NUMBER], row[TAG], row[IDENTIFIER] = parse_filename(filename)

        # must use this encoding to avoid errors
        file_contents = read_file(filepath)
        match = re.search(r"CONTRACT\s+NUMBER\s+([A-Za-z0-9-]+)", file_contents)
        
        if match:
            row[CONTRACT_TYPE] = ContractType.TYPE1.value
            folder = LINEPRINTER_TXT_FILES
        else:
            row[CONTRACT_TYPE] = ContractType.TYPE2.value
            folder = TABLE_TXT_FILES
            
        contract_types.append(row)
        
    df = pd.DataFrame(contract_types)
    df.set_index('Filename', inplace=True)
    df.to_csv('data/contract_types.csv', index=True)
    

def get_contract_types() -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Use as:
    d = get_contract_types()
    d['01-1234.pdf_1']  # return 1 or 2
    """
    df = pd.read_csv('data/contract_types.csv')
    df.set_index('Filename', inplace=True)
    return df, df['Contract_Type'].to_dict()


def get_some_contracts(contract_type=ContractType.TYPE1, num_contracts=5, seed=42):
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
    
    def __init__(self, filepaths, add_timestamp=True, tag="run", contract_type=ContractType.TYPE1):
        self.filepaths = filepaths
        self.contract_type = contract_type
        
        if add_timestamp:
            timestamp = datetime.strftime(datetime.now(), '%m-%d-%Y-%H:%M:%S')
        else:
            timestamp = ''
        
        # Define result path for this specific experiment
        
        if add_timestamp and tag:
            results_filename = f'{timestamp}_tag:_{tag}_type:_{contract_type.value}'
        elif add_timestamp:
            results_filename = f'{timestamp}'
        elif tag:
            results_filename = f'{tag}'
        else:
            self.results_path = RESULTS_PATH 
            
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
            if i % 100 == 0:
                print(f"Processing {i+1}/{n} ... ")
            try:
                contract = Contract(filepath, self.contract_type)
                contract.extract()
                
                self.df_info = pd.concat([self.df_info, contract.info.df])
                self.df_bids = pd.concat([self.df_bids, contract.bids.df])
                self.df_subcontractors = pd.concat([self.df_subcontractors, contract.subcontractors.df])
                self.df_items = pd.concat([self.df_items, contract.items.df])
                
            except Exception as e:
                filename = filepath.stem
                _, _, identifier = parse_filename(filename)
                print({ERROR_FILENAME: filename, ERROR: e})
                self.df_errors = pd.concat([self.df_errors, pd.DataFrame([{ERROR_FILENAME: filename, IDENTIFIER: identifier, ERROR: str(e), CONTRACT_TYPE: str(self.contract_type.value)}])])
                
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
