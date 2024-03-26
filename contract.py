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
TAG = "Tag"
RELATIVE_PATH = "Relative_Path"
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


def parse_filename(filename:str) -> Tuple[str, str]:
    pattern = re.compile(r"^(\d{2}-\w+)\.pdf_(\d+)$", re.IGNORECASE)  # IGNORECASE is critical since names might have both PDF and pdf
    match = pattern.search(filename)
    contract_number, tag = match.groups()
    identifier = f"{contract_number}_{tag}"
    return contract_number, tag, identifier
    
    
def read_file(filepath: str):
    # must use this encoding to avoid errors
    with open(filepath, 'r', encoding='ISO-8859-1') as file:
        return file.read()


class Contract:
    def __init__(self, filepath_or_identifier: Path | str, contract_type = ContractType.TYPE1) -> None:
        if isinstance(filepath_or_identifier, str):
            a, b = filepath_or_identifier.split('_')
            path = RAW_DATA_PATH_LINEPRINTER if contract_type.name == ContractType.TYPE1.name else RAW_DATA_PATH_TABLE
            filepath = path / (a + '.pdf_' + b + '.txt')
        else:
            filepath = filepath_or_identifier
        
        self.filepath = filepath
        self.filename = filepath.stem
        self.contract_number, self.tag, self.identifier = parse_filename(self.filename)
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
        RESULTS_PATH_SINGLE_CONTRACTS.mkdir(exist_ok=True, parents=True)
        path = RESULTS_PATH_SINGLE_CONTRACTS / (self.identifier + '.xlsx')
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            for obj in (self.info, self.bids, self.subcontractors, self.items):
                # Write the DataFrame to a new sheet in the Excel file using the file name as the sheet name
                obj.df.to_excel(writer, sheet_name=obj.__class__.__name__, index=False)
                
        print(f"Saved to Excel file at: {path}.")


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
                            raise ValueError(f'MAJOR ERROR in contract {identifier}. Could not find A+B) line for BID_RANK number: {row[BID_RANK]}')
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


