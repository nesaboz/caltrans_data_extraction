from pathlib import Path
from pprint import pprint
from typing import List, Tuple
from collections import defaultdict
import re
import pandas as pd
import re
from tqdm import tqdm
import numpy as np
import os
from datetime import datetime
import shutil
from dotenv import load_dotenv
from enum import Enum


FILENAME = "Filename"
RELATIVE_FOLDER = "Relative_Folder"
CONTRACT_TYPE = "Contract_Type"

IDENTIFIER = "Identifier"
POSTPONED_CONTRACT = "Postponed_Contract"
NUMBER_OF_BIDDERS = "Number_of_Bidders"
BID_OPENING_DATE = "Bid_Opening_Date"
CONTRACT_DATE = "Contract_Date"
CONTRACT_NUMBER = "Contract_Number"
TOTAL_NUMBER_OF_WORKING_DAYS = "Total_Number_of_Working_Days"
CONTRACT_ITEMS = "Number_of_Contract_Items"
CONTRACT_DESCRIPTION = "Contract_Description"
PERCENT_OVER_EST = "Percent_Est_Over"
PERCENT_UNDER_EST = "Percent_Est_Under"
ENGINEERS_EST = "Engineers_Est"
AMOUNT_OVER = "Amount_Over"
AMOUNT_UNDER = "Amount_Under"
CONTRACT_CODE = "Contract_Code"

IDENTIFIER = "Identifier"
BID_RANK = "Bid_Rank"
A_PLUS_B_INDICATOR = "A_plus_B_indicator"
BID_TOTAL = "Bid_Total"   
BIDDER_ID = "Bidder_ID"
BIDDER_NAME = "Bidder_Name"
BIDDER_PHONE = "Bidder_Phone"
EXTRA = "Extra"
CSLB_NUMBER = "CSLB_Number"
HAS_THIRD_ROW = "Has_Third_Row"
CONTRACT_NOTES = 'Contract_Notes'

ITEM_NUMBER = "Item_Number"
ITEM_CODE = "Item_Code"
ITEM_DESCRIPTION = "Item_Description"
ITEM_DOLLAR_AMOUNT = "Item_Dollar_Amount"
EXTRA1 = "Extra1"
EXTRA2 = "Extra2"
COULD_NOT_PARSE = "COULD NOT PARSE"
ITEM_NUMBERS = "Item_Numbers"
PERCENT = "Percent"

BIDDER_ID = "Bidder_ID"
SUBCONTRACTOR_NAME = "Subcontractor_Name"
SUBCONTRACTED_LINE_ITEM = "Subcontracted_Line_Item"
BIDDER_ID1 = "Bidder_ID1"
SUBCONTRACTOR_NAME1 = "Subcontractor_Name1"
SUBCONTRACTED_LINE_ITEM1 = "Subcontracted_Line_Item1"
CITY = "City"
SUBCONTRACTOR_LICENSE_NUMBER = "Subcontractor_License_Number"

ERROR_FILENAME = "Error_Filename"
ERROR = "Error"

class ContractType(Enum):
    TYPE1 = 1
    TYPE2 = 2


class SampleSize(Enum):
    SMALL = 1  # say 10 files
    FULL = 2
    
    
def get_raw_data_path():
    load_dotenv()
    raw_data_path = Path(os.getenv('RAW_DATA_PATH'))
    if not raw_data_path.exists():
        raise ValueError('Make sure to set a path to raw data in the .env file or copy data into root of the repo')
    return raw_data_path
    
RAW_DATA_PATH = get_raw_data_path()
LINEPRINTER_TXT_FILES = 'lineprinter_txt_files'
TABLE_TXT_FILES = 'table_txt_files'
RAW_DATA_PATH_LINEPRINTER = RAW_DATA_PATH / LINEPRINTER_TXT_FILES
RAW_DATA_PATH_TABLE = RAW_DATA_PATH / TABLE_TXT_FILES
RESULTS_PATH = Path('results')
RESULTS_PATH_SINGLE_CONTRACTS = RESULTS_PATH / 'single_contracts'
RESULTS_PATH_SINGLE_CONTRACTS.mkdir(exist_ok=True, parents=True)
    
    
def read_file(filepath: str):
    # must use this encoding to avoid errors
    with open(filepath, 'r', encoding='ISO-8859-1') as file:
        return file.read()
    
    
def save_contract_types():
    """
    Inspect quickly contract to determine if it is of type 1 or 2.
    """
    filepaths = RAW_DATA_PATH_LINEPRINTER.glob('*.txt')

    contract_types = []
    for i, filepath in enumerate(filepaths):
        filename = filepath.name
        row = defaultdict(str)
        row[FILENAME] = filename
        
        # must use this encoding to avoid errors
        file_contents = read_file(filepath)
        
        match = re.search(r"CONTRACT\s+NUMBER\s+([A-Za-z0-9-]+)", file_contents)
        
        if match:
            row[CONTRACT_TYPE] = ContractType.TYPE1.value
            folder = LINEPRINTER_TXT_FILES
        else:
            row[CONTRACT_TYPE] = ContractType.TYPE2.value
            folder = TABLE_TXT_FILES
            
        row[RELATIVE_FOLDER] = os.path.join(folder, filename)
        
        contract_types.append(row)
        
    df = pd.DataFrame(contract_types)
    df.set_index('Filename', inplace=True)
    df.to_csv('data/contract_types.csv', index=True)
    

def get_contract_types():
    """Use as:
    d = get_contract_types()
    d['01-1234.pdf_1']  # return 1 or 2
    """
    df = pd.read_csv('data/contract_types.csv')
    df.set_index('Filename', inplace=True)
    return df, df['Contract_Type'].to_dict()


class Contract:
    def __init__(self, filepath: Path | str) -> None:
        if isinstance(filepath, str):
            # this means it's an identifier
            c1, t1 = filepath.split('_')
            filepath = RAW_DATA_PATH_LINEPRINTER / (c1 + '.pdf_' + t1 + '.txt')
        
        self.filepath = filepath
        self.filename = filepath.stem
        self.contract_number, self.tag, self.identifier = self.get_contract_number_and_tag_from_filename(self.filename)
        self._file_contents = read_file(self.filepath)
        
        self.info = Info(self.file_contents, self.identifier)
        self.bids = Bids(self.file_contents, self.identifier)
        self.subcontractors = Subcontractors(self.file_contents, self.identifier)
        self.items = Items(self.file_contents, self.identifier)
        
    def extract(self):
        self.info.extract()
        self.bids.extract()
        self.subcontractors.extract()
        self.items.extract()
        
    def get_contract_number_and_tag_from_filename(self, filename:str) -> Tuple[str, str]:
        pattern = re.compile(r"^(\d{2}-\w+)\.pdf_(\d+)$", re.IGNORECASE)  # IGNORECASE is critical since names might have both PDF and pdf
        match = pattern.search(filename)
        contract_number, tag = match.groups()
        identifier = f"{contract_number}_{tag}"
        return contract_number, tag, identifier
    
    @property
    def file_contents(self):
        return self._file_contents
    
    def copy_file(self, to_folder):
        shutil.copy(
            str(self.filepath), 
            to_folder / f'{self.filename}.txt'
            )

    def write_to_excel(self):
        # Create a Pandas Excel writer using openpyxl as the engine
        with pd.ExcelWriter(RESULTS_PATH_SINGLE_CONTRACTS / (self.identifier + '.xlsx'), engine='openpyxl') as writer:
            for obj in (self.info, self.bids, self.subcontractors, self.items):
                # Write the DataFrame to a new sheet in the Excel file using the file name as the sheet name
                obj.df.to_excel(writer, sheet_name=obj.__class__.__name__, index=False)


class ContractPortionBase(object):
    def __init__(self, file_contents, identifier) -> None:
        self.file_contents = file_contents
        self.identifier = identifier
        self.rows = None
        self._df = None
    
    @property
    def df(self):
        return self._df
        
    def preprocess(self, regex: str) -> List[str]:
        """
        Uses regex to narrow down the file_contents to specific sections that will be returned.
        """
        pattern = re.compile(regex)
        matches = pattern.findall(self.file_contents)
        return matches if matches else []
    
    def _parse(self, text: str, identifier: str):
        raise NotImplementedError
    
    @staticmethod
    def postprocess(df):
        """
        Optional postprocessing of extracted DataFrame.
        """
        raise NotImplementedError
    
    def extract(self):
        if self.NARROW_REGEX:
            matches = self.preprocess(self.NARROW_REGEX)
            
        processed_lines = []
        for match in matches:
            rows = self._parse(match, self.identifier)
            processed_lines.extend(rows)
    
        self.rows = processed_lines
        self._df = pd.DataFrame(self.rows)
        
        if self.rows and 'postprocess' in self.__class__.__dict__:  # this checks if postprocess is implemented in the class
            self._df = self.postprocess(self._df)


class Info(ContractPortionBase):
    
    COLUMNS = [IDENTIFIER, POSTPONED_CONTRACT, NUMBER_OF_BIDDERS, BID_OPENING_DATE, 
               CONTRACT_DATE, CONTRACT_NUMBER, TOTAL_NUMBER_OF_WORKING_DAYS, CONTRACT_ITEMS, 
               CONTRACT_DESCRIPTION, PERCENT_OVER_EST, PERCENT_UNDER_EST, ENGINEERS_EST, 
               AMOUNT_OVER, AMOUNT_UNDER, CONTRACT_CODE]
        
    # narrow from the beginning of the file to the first occurrence of BID RANK
    NARROW_REGEX = r'(?s)^.*?(?:BID RANK)'
    
    @staticmethod
    def _parse(text: str, identifier: str):
        
        def _extract(regex, groups=(1,)):
            # Search for the pattern in the text
            match = re.search(regex, text)
            if match:
                if len(groups) == 1:
                    return match.group(groups[0])
                else:
                    return (match.group(i) for i in groups)
            else:
                return ""
        
        row = defaultdict(str)
        row[IDENTIFIER] = identifier
        row[POSTPONED_CONTRACT] = int(bool(_extract(r"(POSTPONED CONTRACT)")))
        row[BID_OPENING_DATE], row[CONTRACT_DATE] = _extract(r"BID OPENING DATE\s+(\d+\/\d+\/\d+).+\s+(\d+\/\d+\/\d+)", (1, 2))
        row[CONTRACT_NUMBER] = _extract(r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")
        row[CONTRACT_CODE] = _extract(r"CONTRACT CODE\s+'([^']+)'").strip()
        row[CONTRACT_ITEMS] = _extract(r"(\d+)\s+CONTRACT ITEMS")
        row[TOTAL_NUMBER_OF_WORKING_DAYS] = _extract(r"TOTAL NUMBER OF WORKING DAYS\s+(\d+)")
        row[NUMBER_OF_BIDDERS] = _extract(r"NUMBER OF BIDDERS\s+(\d+)")
        row[ENGINEERS_EST] = _extract(r"ENGINEERS EST\s+([\d,]+\.\d{2})")
        row[AMOUNT_OVER] = _extract(r"AMOUNT OVER\s+([\d,]+\.\d{2})")
        row[AMOUNT_UNDER] = _extract(r"AMOUNT UNDER\s+([\d,]+\.\d{2})")
        row[PERCENT_OVER_EST] = _extract(r"PERCENT OVER EST\s+(\d+.\d{2})")
        row[PERCENT_UNDER_EST] = _extract(r"PERCENT UNDER EST\s+(\d+.\d{2})")
        row[CONTRACT_DESCRIPTION] = _extract(r"(?:\n)?(.*?)FEDERAL AID").strip()
        processed_lines = [row]
        return processed_lines


class Bids(ContractPortionBase):
    
    NARROW_REGEX = r"(?s)BID RANK\s+BID TOTAL\s+BIDDER ID\s+BIDDER INFORMATION\s+\(NAME\/ADDRESS\/LOCATION\)(.*?)(?=L I S T   O F   S U B C O N T R A C T O R S)"
    
    @staticmethod
    def _parse(text, identifier):
        """
        Parses a table from a text line by line, works even if BIDER NAME has 3 rows (very rare). Just having a regex pattern does not work since columns are very variable in width, and we need
        extra logic, hence, we parse line by line in 3 steps.
        """
        pattern = re.compile(r"^\s+(\d+)\s+(A\))?\s+([\d,]+\.\d{2})\s+(\d+)\s+(.+)(\d{3} \d{3}-\d{4})(.*)?")
        lines = text.split('\n')
        
        i = 0
        
        n = len(lines)
        processed_lines = []
        while i < n:
            match = re.match(pattern, lines[i])
            if match:
                # this mean we hit the first line, lets parse it and save it
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                row[BID_RANK] = match.group(1)
                row[A_PLUS_B_INDICATOR] = 1 if match.group(2) else 0
                row[BID_TOTAL] = match.group(3)
                row[BIDDER_ID] = match.group(4).strip()
                row[BIDDER_NAME] = match.group(5).strip()
                row[BIDDER_PHONE] = match.group(6).strip()
                row[EXTRA] = match.group(7)
                    
                # moving onto the second line:    
                i += 1
                name_starts = match.start(5)
                name_ends = match.end(5)
                delta = name_ends - name_starts
                pattern_cslb_number = re.compile(rf"^(.{{{name_starts}}})(.{{{delta}}})(\d+)$")
                match_cslb_number = re.match(pattern_cslb_number, lines[i])
                
                if match_cslb_number:
                    # this should be the case if second line is present
                    row[CONTRACT_NOTES] = match_cslb_number.group(1).strip()
                    row[BIDDER_NAME] += match_cslb_number.group(2).rstrip()  # this is the second line of the bidder name
                    row[CSLB_NUMBER] = match_cslb_number.group(3)
                else:
                    # log as error:
                    raise ValueError(f'Second line is not a standard format (notes, extra name, CLBS number)')
                
                # moving onto the next line, here it might be a third line or an address followed by a FAX number:
                i += 1
                pattern_fax_number = re.compile(r"^.*(FAX|;).*(\d{3} \d{3}-\d{4}).*$")
                match_fax_number = re.match(pattern_fax_number, lines[i])
                if not match_fax_number:
                    # this means we have a third line, so just strip and add to the name
                    row[BIDDER_NAME] += lines[i].strip()
                    row[HAS_THIRD_ROW] = 1
                else:
                    # we don't have a third row
                    row[HAS_THIRD_ROW] = 0
                    # if there is A) then let's also keep moving until we find the A+B) line
                    if row[A_PLUS_B_INDICATOR]:
                        pattern_a_plus_b = re.compile(r".*A\+B\)\s+([\d,]+\.\d{2}).*")
                        while i < n:
                            match_a_plus_b = re.match(pattern_a_plus_b, lines[i])
                            if match_a_plus_b:
                                row[BID_TOTAL] = match_a_plus_b.group(1)
                                break
                            i += 1
                        if i == n:
                            processed_lines.append(row)
                            raise ValueError(f'MAJOR ERROR in contract {identifier}. Could not find A+B) line for BID_RANK {row[BID_RANK]}')
                processed_lines.append(row)
            i += 1  # we can allow this double jump since there is always empty line between the rows
        return processed_lines


class Subcontractors(ContractPortionBase):
    
    COLUMNS = [IDENTIFIER, BIDDER_ID, SUBCONTRACTOR_NAME, SUBCONTRACTED_LINE_ITEM, CITY, SUBCONTRACTOR_LICENSE_NUMBER]
    
    # NARROW_REGEX = r"(?s)BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED(.*?)(?=BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED|\f|CONTINUED ON NEXT PAGE)"
    NARROW_REGEX = r"(?sm)^([^\S\r\n]*BIDDER ID NAME AND ADDRESS\s+(?:LICENSE NUMBER)?\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED)(.*?)(?=[^\S\r\n]*BIDDER ID NAME AND ADDRESS\s+(?:LICENSE NUMBER)?\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED|\f|CONTINUED ON NEXT PAGE)"
    # NARROW_REGEX = r"(?s)(BIDDER ID)\s+(NAME AND ADDRESS)\s+(LICENSE NUMBER)?\s+(DESCRIPTION OF PORTION OF WORK SUBCONTRACTED)"
    
    @staticmethod
    def _parse(header_and_text, identifier):
        
        header, text = header_and_text
        r = re.match(r"(?s)\s*(BIDDER ID)\s+(NAME AND ADDRESS)\s+(LICENSE NUMBER)?\s+(DESCRIPTION OF PORTION OF WORK SUBCONTRACTED)", header)
        lines = text.split('\n')
        
        delta = r.start(4) - r.start(2)
        i = 0
        processed_lines = []
        while i < len(lines) - 1:
            line = lines[i]
            match = re.match(rf"^\s+(\d+)?\s+(.{{{delta}}})\s+(.+)$", line)
            if match:
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                
                row[BIDDER_ID] = line[r.start(1):r.end(1)].strip()
                row[SUBCONTRACTOR_NAME] = line[r.start(2):r.start(4)].strip()
                row[SUBCONTRACTED_LINE_ITEM] = line[r.start(4):].strip()
                
                # alternate approach
                row[BIDDER_ID1] = match.group(1)
                row[SUBCONTRACTOR_NAME1] = match.group(2).strip()
                row[SUBCONTRACTED_LINE_ITEM1] = match.group(3).strip()
                
                if not any([x in row[SUBCONTRACTED_LINE_ITEM] for x in ("PER BID ITEM", "WORK AS DESCRIBED BY BID ITEM(S) LISTED")]):
                    # attempt parsing
                    matches3 = re.search(r"^.*?(?:ITEMS|ITEM NUMBERS|ITEM\(S\):|ITEM)(.*?)(?:\((.+)\))?$", row[SUBCONTRACTED_LINE_ITEM])
                    if matches3:  # gets two groups, for example: ' 6 THRU 8 AND 13 THRU 15 ', '10%'
                        row[ITEM_NUMBERS] = matches3.group(1).replace('THRU', '-').replace('AND', ',').replace('&', ',')
                        if matches3.group(2):
                            row[PERCENT] = matches3.group(2).replace("%", "")
                
                if r.group(3) is not None:  # there is LICENSE NUMBER column
                    i += 1
                    line = lines[i]
                    row[SUBCONTRACTOR_LICENSE_NUMBER] = line[r.start(3):].strip()
                else:
                    row[SUBCONTRACTOR_LICENSE_NUMBER] = ''
                processed_lines.append(row)
            i += 1
        
        return processed_lines

    @staticmethod
    def postprocess(df):
        # fill gaps in BIDDER_ID
        df[BIDDER_ID] = df[BIDDER_ID].replace('', np.nan)
        df[BIDDER_ID] = df[BIDDER_ID].ffill()
        return df
    
    def _expand_ranges_in_subcontracted_line_item(self, line: str) -> str:
        """
        For example: takes a "6-8, 13-15" and converts to "6, 7, 8, 13, 14, 15".
        Converts NaN to empty string.
        """
        if pd.isnull(line):
            return ""
        
        try:
            # Split the string by commas to separate different ranges/groups
            parts = str(line).split(',')
            # Initialize an empty list to store all numbers
            all_numbers = []
            
            for part in parts:
                # Strip whitespace and check if part contains a range (indicated by '-')
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    # Add all numbers in this range (inclusive) to the list
                    all_numbers.extend(range(start, end + 1))
                else:
                    # If not a range, just add the single number
                    all_numbers.append(int(part.strip()))
            
            return all_numbers
        except:
            return []


class Items(ContractPortionBase):
    
    COLUMNS = [ITEM_NUMBER, EXTRA1, ITEM_CODE, ITEM_DESCRIPTION, EXTRA2, ITEM_DOLLAR_AMOUNT]
    
    NARROW_REGEX = r"(?s)C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R(.*?)(?=C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R|\f|CONTINUED ON NEXT PAGE)"
    
    @staticmethod
    def _parse(text: str, identifier: str):
        """
        Parses a table from a text line by line.
        """
        pattern = re.compile(r'^\s+(\d+)\s+(?:\((F|SF|S)\))?\s*(\d+)\s+(.{45})\s+(.*)\s+([\d,]+\.\d{2})')
        lines = text.split('\n')
        
        i = 0
        
        n = len(lines)
        processed_lines = []
        row = None
        first_line = False
        while i < n:
            line = lines[i]
            match = re.match(pattern, line)
            if match:
                # this mean we hit the first line, lets parse it and save it
                # but first we need to save any previous line to precessed_lines
                if row:
                    processed_lines.append(row)
                
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                row[ITEM_NUMBER] = match.group(1)
                row[EXTRA1] = match.group(2)
                row[ITEM_CODE] = match.group(3)
                row[ITEM_DESCRIPTION] = match.group(4).strip()
                row[EXTRA2] = match.group(5)
                row[ITEM_DOLLAR_AMOUNT] = match.group(6)
                first_line = True
            elif row and first_line:
                # this means we have a second line, let's append it to the ITEM_DESCRIPTION
                row[ITEM_DESCRIPTION] += line.strip()
                first_line = False
            i += 1  
        
        # save last line
        if row:
            processed_lines.append(row)  
        return processed_lines


class Experiment:
    """
    We should be able to run this on a small sample size or full, as well as type1 or type2.
    Also have an optional tag, and add timestamp to the results.
    
    The results folder will be determined automatically.
    """
    
    def __init__(self, filepaths, add_timestamp=False, tag=None):
        self.filepaths = filepaths
        if add_timestamp:
            timestamp = datetime.strftime(datetime.now(), '%m-%d-%Y-%H:%M:%S')
        else:
            timestamp = ''
        
        # Define result path for this specific experiment
        if add_timestamp or tag:
            self.results_path = RESULTS_PATH / ((f'{timestamp}_' if timestamp else '') + (f'_{tag}' if tag else ''))
        else:
            self.results_path = RESULTS_PATH 
        self.outliers_path = self.results_path / 'outliers'
        
        # Create the results folders
        self.results_path.mkdir(exist_ok=True, parents=True)
        self.outliers_path.mkdir(exist_ok=True, parents=True)
        self.outliers_path.mkdir(exist_ok=True, parents=True)
    
    def write_to_results(self, df: pd.DataFrame | List, name: str):
        if isinstance(df, list):
            df = pd.DataFrame(df)
        df.to_csv(self.results_path / f'{name}.csv', index=False)
    
    def write_to_excel(self):
        # Paths to your CSV files
        csv_file_paths = self.results_path.glob('*.csv')

        # Path to the output Excel file
        excel_file_path = self.results_path / 'results.xlsx'

        # Create a Pandas Excel writer using openpyxl as the engine
        with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
            # Iterate over your CSV files
            for csv_file in tqdm(csv_file_paths):
                # Use Path from pathlib to work with file paths
                csv_path = Path(csv_file)
                
                # Extract the file name without the extension for the sheet name
                sheet_name = csv_path.name
                
                # Read each CSV file into a DataFrame
                try:
                    df = pd.read_csv(csv_file)
                except pd.errors.EmptyDataError:
                    continue
                
                # Write the DataFrame to a new sheet in the Excel file using the file name as the sheet name
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f'Merged CSV files into {excel_file_path}')
    
    def run(self):
        """
        Run a batch or a single file (by making `files` a single element list).
        """
        
        df_contract_data = pd.DataFrame()
        df_contract_bid_data = pd.DataFrame()
        df_bid_subcontractor_data = pd.DataFrame()
        df_contract_line_item_data = pd.DataFrame()
        
        error_files = []
        
        for filepath in tqdm(self.filepaths):
            
            try:
                filename = filepath.stem
                contract = Contract(filename)
                
                contract_data = Info(contract)
                contract_bid_data = Bids(contract)
                bid_subcontractor_data = Subcontractors(contract)
                contract_line_item_data = Items(contract)
                
                contract_data.extract()
                contract_bid_data.extract()
                bid_subcontractor_data.extract()
                contract_line_item_data.extract()
                
                df_contract_data = pd.concat([df_contract_data, contract_data.df])
                df_contract_bid_data = pd.concat([df_contract_bid_data, contract_bid_data.df])
                df_bid_subcontractor_data = pd.concat([df_bid_subcontractor_data, bid_subcontractor_data.df])
                df_contract_line_item_data = pd.concat([df_contract_line_item_data, contract_line_item_data.df])
                
            except Exception as e:
                print({ERROR_FILENAME: filename, ERROR: e})
                error_files.append({ERROR_FILENAME: filename, ERROR: e})
                
        self.write_to_results(df_contract_data, "contract_data")
        self.write_to_results(df_contract_bid_data, "contract_bid_data")
        self.write_to_results(df_bid_subcontractor_data, "bid_subcontractor_data")
        self.write_to_results(df_contract_line_item_data, "contract_line_item_data")
        self.write_to_results(error_files, "errors")
