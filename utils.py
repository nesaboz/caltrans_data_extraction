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
import random


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


class ContractBase(object):
    def __init__(self, contract) -> None:
        self.contract = contract
        self.rows = None
        self._df = None
    
    def pre_process(self):
        pass
    
    @property
    def df(self):
        return self._df
        
    def narrow_file_contents(self, regex: str) -> List[str]:
        """
        Uses regex to narrow down the file_contents to specific sections that will be returned.
        """
        pattern = re.compile(regex)
        matches = pattern.findall(self.contract.file_contents)
        return matches if matches else []


class ContractData(ContractBase):
    
    COLUMNS = [IDENTIFIER, POSTPONED_CONTRACT, NUMBER_OF_BIDDERS, BID_OPENING_DATE, 
               CONTRACT_DATE, CONTRACT_NUMBER, TOTAL_NUMBER_OF_WORKING_DAYS, CONTRACT_ITEMS, 
               CONTRACT_DESCRIPTION, PERCENT_OVER_EST, PERCENT_UNDER_EST, ENGINEERS_EST, 
               AMOUNT_OVER, AMOUNT_UNDER, CONTRACT_CODE]
        
    def _extract(self, regex, groups=(1,)):
        # Search for the pattern in the text
        match = re.search(regex, self.contract.file_contents)
        if match:
            if len(groups) == 1:
                return match.group(groups[0])
            else:
                return (match.group(i) for i in groups)
        else:
            return ""
    
    def extract(self):
        row = defaultdict(str)
        row[IDENTIFIER] = self.contract.identifier
        row[POSTPONED_CONTRACT] = int(bool(self._extract(r"(POSTPONED CONTRACT)")))
        row[BID_OPENING_DATE], row[CONTRACT_DATE] = self._extract(r"BID OPENING DATE\s+(\d+\/\d+\/\d+).+\s+(\d+\/\d+\/\d+)", (1, 2))
        row[CONTRACT_NUMBER] = self._extract(r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")
        row[CONTRACT_CODE] = self._extract(r"CONTRACT CODE\s+'([^']+)'").strip()
        row[CONTRACT_ITEMS] = self._extract(r"(\d+)\s+CONTRACT ITEMS")
        row[TOTAL_NUMBER_OF_WORKING_DAYS] = self._extract(r"TOTAL NUMBER OF WORKING DAYS\s+(\d+)")
        row[NUMBER_OF_BIDDERS] = self._extract(r"NUMBER OF BIDDERS\s+(\d+)")
        row[ENGINEERS_EST] = self._extract(r"ENGINEERS EST\s+([\d,]+\.\d{2})")
        row[AMOUNT_OVER] = self._extract(r"AMOUNT OVER\s+([\d,]+\.\d{2})")
        row[AMOUNT_UNDER] = self._extract(r"AMOUNT UNDER\s+([\d,]+\.\d{2})")
        row[PERCENT_OVER_EST] = self._extract(r"PERCENT OVER EST\s+(\d+.\d{2})")
        row[PERCENT_UNDER_EST] = self._extract(r"PERCENT UNDER EST\s+(\d+.\d{2})")
        row[CONTRACT_DESCRIPTION] = self._extract(r"(?:\n)?(.*?)FEDERAL AID").strip()
        self.rows = [row]
        self._df = pd.DataFrame(self.rows)


class BidData(ContractBase):
        
    def _parse(self):
        """
        Parses a table from a text line by line, works even if BIDER NAME has 3 rows (very rare).
        
        Old method that used only regex failed to parse if BIDER NAME has 3 rows.
        It had a long regex:
        """
        pattern1 = re.compile(r"^\s+(\d+)\s+(A\))?\s+([\d,]+\.\d{2})\s+(\d+)\s+(.+)\s(\d{3} \d{3}-\d{4})(.*)?")
        # (^\s*)(\d+)(\s+)(A\))?(\s*)([\d,]+\.\d{2})(\s+)(\d+)(\s*)(.{37})\s(\d{3} \d{3}-\d{4})(.*)?
        
        pattern2 = re.compile(r"^(.{65})(.{38})\s+(\d{8})$")
        pattern3 = re.compile(r"^.+(FAX|B\)|\d{3} \d{3}-\d{4}|;).*+$")
        
        lines = self.contract.file_contents.split('\n')

        processed_lines = []
        just_passed_first_line = False
        just_passed_second_line = False
        just_passed_third_line = False
        row = None
        for i, line in enumerate(lines):
            match1 = re.match(pattern1, line)
            match2 = re.match(pattern2, line)
            match3 = re.match(pattern3, line)
            if match1:
                # write a previous row if it exists
                if row:
                    if HAS_THIRD_ROW not in row:
                        row[HAS_THIRD_ROW] = 0
                    processed_lines.append(row)
                
                match = match1.groups()
                row = defaultdict(str)
                row[IDENTIFIER] = self.contract.identifier
                row[BID_RANK] = match[0]
                row[A_PLUS_B_INDICATOR] = 1 if match[1] else 0
                row[BID_TOTAL] = match[2]
                row[BIDDER_ID] = match[3].strip()
                row[BIDDER_NAME] = match[4].strip()
                row[BIDDER_PHONE] = match[5].strip()
                row[EXTRA] = match[6]
            
                just_passed_first_line = True
                just_passed_second_line = False
                just_passed_third_line = False
                
            elif match2 and just_passed_first_line:
                match = match2.groups()
                
                row[CONTRACT_NOTES] = match[0].strip()
                row[BIDDER_NAME] += (' ' + match[1].strip()).strip()  # this is the second line of the bidder name
                row[CSLB_NUMBER] = match[2]
                
                just_passed_first_line = False
                just_passed_second_line = True
                just_passed_third_line = False
            
            elif match3 is None and just_passed_second_line:
                # this means we are on the third line and it does not have some keyword from pattern3 in it
                # then just add this line to the name
                row[BIDDER_NAME] += (' ' + line.strip()).strip()
                row[HAS_THIRD_ROW] = 1
                just_passed_first_line = False
                just_passed_second_line = False
                just_passed_third_line = True
            else:
                just_passed_first_line = False
                just_passed_second_line = False
                just_passed_third_line = False

        if row:
            if HAS_THIRD_ROW not in row:
                row[HAS_THIRD_ROW] = 0
            processed_lines.append(row)   
        
        return processed_lines

    def extract(self):
        contract_bid_data = self._parse()

        # if contract has A+B we need to correct the BID_TOTAL:
        # the following will find many A+B) matches but it is reasonable to expect that first A+B) matches are all we need
        pattern = re.compile(r"A\+B\)\s+([\d,]+\.\d{2})", re.MULTILINE)  
        a_plus_b_bids = pattern.findall(self.contract.file_contents)
        if a_plus_b_bids:
            for i, a_plus_b_bid in zip(range(len(contract_bid_data)), a_plus_b_bids):  # this does truncation of a_plus_b_bids list 
                contract_bid_data[i][BID_TOTAL] = a_plus_b_bid

        self.rows = contract_bid_data
        self._df = pd.DataFrame(self.rows)


class SubcontractorData(ContractBase):
    
    COLUMNS = [IDENTIFIER, BIDDER_ID, SUBCONTRACTOR_NAME, SUBCONTRACTED_LINE_ITEM, CITY, SUBCONTRACTOR_LICENSE_NUMBER]

    def extract(self):
        """
        We extract data in two steps.
        1) First we get the relevant information from a whole contract using pattern1:
        "X(.*?)(?=X|Y|Z)"
        this means starting phrase must be X, then text that we want extracted and then the match can either finish with X, Y or Z.
        In our case:
        X = BIDDER ID NAME AND ADDRESS LICENSE NUMBER DESCRIPTION OF PORTION OF WORK SUBCONTRACTED
        Y = \f (this is a new page character, in the text is denoted as FF, but this is not a pure FF text but /f)
        Z = CONTINUED ON NEXT PAGE

        I also ensure that we are doing positive lookahead (using ?=), so the matches do not overlap.

        2) The second step is to exact the columns, we use some fixed with columns for that in pattern2.
        """

        matches1 = self.narrow_file_contents(
            r"(?s)BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED(.*?)(?=BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED|\f|CONTINUED ON NEXT PAGE)"
            )
            
        pattern2 = re.compile(r"(?m)^\s+(\d{2})?\s+(.{58})\s+(.+)\n\s+(.{38})?(.+)")
                
        bid_subcontractor_data = []
        for match1 in matches1:
            matches2 = pattern2.findall(match1)
            for match2 in matches2:
                row = defaultdict(str)
                row[IDENTIFIER] = self.contract.identifier
                row[BIDDER_ID] = match2[0]
                row[SUBCONTRACTOR_NAME] = match2[1].strip()
                row[SUBCONTRACTED_LINE_ITEM] = match2[2]
                row[CITY] = match2[3].strip()
                row[SUBCONTRACTOR_LICENSE_NUMBER] = match2[4].strip()

                if not any([x in row[SUBCONTRACTED_LINE_ITEM] for x in ("PER BID ITEM", "WORK AS DESCRIBED BY BID ITEM(S) LISTED")]):
                    # attempt parsing
                    matches3 = re.search(r"^.*?(?:ITEMS|ITEM NUMBERS|ITEM\(S\):|ITEM)(.*?)(?:\((.+)\))?$", row[SUBCONTRACTED_LINE_ITEM])
                    if matches3:  # gets two groups, for example: ' 6 THRU 8 AND 13 THRU 15 ', '10%'
                        row[ITEM_NUMBERS] = matches3.group(1).replace('THRU', '-').replace('AND', ',').replace('&', ',')
                        if matches3.group(2):
                            row[PERCENT] = matches3.group(2).replace("%", "")
                            
                bid_subcontractor_data.append(row)

        self.rows = bid_subcontractor_data
        _df = pd.DataFrame(self.rows)
        
        if self.rows:
            self._df = self._fill_gaps_in_bidder_id(_df)
        
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

    def _fill_gaps_in_bidder_id(self, df):
        df[BIDDER_ID] = df[BIDDER_ID].replace('', np.nan)
        df[BIDDER_ID] = df[BIDDER_ID].ffill()
        return df


class LineItemData(ContractBase):
    
    COLUMNS = [ITEM_NUMBER, EXTRA1, ITEM_CODE, ITEM_DESCRIPTION, EXTRA2, ITEM_DOLLAR_AMOUNT]
        
    def _parse_table(self, text: str, regex: str, regex_tag: str):
            """
            Parses a table from a text line by line.
            """
            pattern = re.compile(regex, re.MULTILINE)
            
            lines = text.split('\n')

            processed_lines = []
            just_passed_first_line = False
            for i, line in enumerate(lines):
                if re.match(pattern, line):
                    processed_lines.append(line)
                    just_passed_first_line = True
                else:
                    if just_passed_first_line:
                        # append process the following line
                        processed_lines[-1] = processed_lines[-1] + ' ' + line.strip()
                        just_passed_first_line = False

            # Now, apply the regex to each processed line
            pattern2 = re.compile(regex + regex_tag)
            matches = [re.match(pattern2, line) for line in processed_lines]
            
            return matches

    def extract(self):
        
        matches1 = self.narrow_file_contents(
            r"(?s)C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R(.*?)(?=C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R|\f|CONTINUED ON NEXT PAGE)"
            )
        
        regex = r'^\s+(\d+)\s+(?:\((F|SF|S)\))?\s*(\d+)\s+(.{45})\s+(.{35})\s+([\d,]+\.\d{2})'
        regex_tag = r'(.*+)'

        contract_line_item_data = []
        for match1 in matches1:
            matches2 = self._parse_table(match1, regex, regex_tag)
            for match2 in matches2:
                row = defaultdict(str)
                row[IDENTIFIER] = self.contract.identifier
                row[ITEM_NUMBER] = match2[1]
                row[EXTRA1] = match2[2]
                row[ITEM_CODE] = match2[3]
                row[ITEM_DESCRIPTION] = match2[4].strip() + ' ' + match2[7]
                row[EXTRA2] = match2[5]
                row[ITEM_DOLLAR_AMOUNT] = match2[6]
                
                contract_line_item_data.append(row)
        
        self.rows = contract_line_item_data
        self._df = pd.DataFrame(self.rows)


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
                
                contract_data = ContractData(contract)
                contract_bid_data = BidData(contract)
                bid_subcontractor_data = SubcontractorData(contract)
                contract_line_item_data = LineItemData(contract)
                
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
