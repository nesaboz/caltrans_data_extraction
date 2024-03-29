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
PERCENT_OVER_UNDER_EST = "Percent_Over_Under_Est"
PERCENT_OVER_EST = "Percent_Est_Over"
PERCENT_UNDER_EST = "Percent_Est_Under"
ENGINEERS_EST = "Engineers_Est"
AMOUNT_OVER_UNDER = "Amount_Over_Under"
AMOUNT_OVER = "Amount_Over"
AMOUNT_UNDER = "Amount_Under"
CONTRACT_CODE = "Contract_Code"

IDENTIFIER = "Identifier"
ORIGINAL_IDENTIFIER = "Original_Identifier"
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
ITEM_FLAG = "Item_Flag"
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
    TYPE3 = 3  # these files were produced by splitting the type1 files
    
    
def get_raw_data_path():
    load_dotenv()
    raw_data_path = Path(os.getenv('RAW_DATA_PATH'))
    if not raw_data_path.exists():
        raise ValueError('Make sure to set a path to raw data in the .env file or copy data into root of the repo')
    return raw_data_path
    

RAW_DATA_PATH = get_raw_data_path()
LINEPRINTER_LABEL = 'lineprinter'
TABLE_LABEL = 'table'

RAW_DATA_PATH_LINEPRINTER = RAW_DATA_PATH / LINEPRINTER_LABEL
RAW_DATA_PATH_TABLE = RAW_DATA_PATH / TABLE_LABEL

TYPE1_PATH = RAW_DATA_PATH / 'type1'
TYPE2_PATH = RAW_DATA_PATH / 'type2'
TYPE3_PATH = RAW_DATA_PATH / 'type3'

contract_type_paths = {ContractType.TYPE1.name: TYPE1_PATH, ContractType.TYPE2.name: TYPE2_PATH, ContractType.TYPE3.name: TYPE3_PATH} 

RESULTS_PATH = Path('results')
RESULTS_PATH_SINGLE_CONTRACTS = RESULTS_PATH / 'single_contracts'
RESULTS_PATH_SINGLE_CONTRACTS.mkdir(exist_ok=True, parents=True)

split_pattern = re.compile(r'State of California Department of Transportation')
parse_filename_pattern = re.compile(r"^(\d{2}-\w+)\.pdf_(\d+)$", re.IGNORECASE)  # IGNORECASE is critical since names might have both PDF and pdf
contract_number_regex = re.compile(r"Contract Number:\s*([\w-]+)")

BIDS_FIRST_LINE_PATTERN = re.compile(r"^(\d+)\s+(A\))?\s+\$([\d,]+\.\d{2})\s*(\w+)\s*(.+)Phone\s*(\(\d{3}\)\d{3}-\d{4})(.*)?")
SUBCONTRACTORS_FIRST_LINE_REGEX = r"[^\S\r\n]*(BIDDER\s+ID)\s+(NAME\s+AND\s+ADDRESS)\s+(LICENSE\s+NUMBER)?\s+(DESCRIPTION\s+OF\s+PORTION\s+OF\s+WORK\s+SUBCONTRACTED)"

no_bids_contract = r"NO\s+BIDS\s+FOR\s+THIS\s+CONTRACT"  # TODO some contracts have this phrase like data/type2/02-0H8004_10657.txt

def get_next_line(i, lines):
    i += 1
    n = len(lines)
    while i < n and lines[i].strip() == '':
        i += 1
    return i
                            
                            
def parse_filename(filename:str) -> Tuple[str, str]:
    match = parse_filename_pattern.search(filename)
    contract_number, tag = match.groups()
    identifier = f"{contract_number}_{tag}"
    return contract_number, tag, identifier
    
    
def read_file(filepath: str):
    # must use this encoding to avoid errors
    with open(filepath, 'r', encoding='ISO-8859-1') as file:
        return file.read()


def split_contract(identifier, file_contents, tag) -> List[Tuple[str, str]]:
    """
    Uses phrase in the header to split the contract into multiple partial_texts, then saves those into separate files where new 
    name is the contract_number + tag. If contract_number + tag is non-original, code skips at reports an issue.
    
    Returns a list of tuples with new identifier, new_file_contents.
    """
    matches = re.finditer(split_pattern, file_contents)

    # Extract and print starting positions
    positions = [match.start() for match in matches] + [None]

    splits = []
    new_identifiers = []
    for i in range(len(positions) - 1):
        new_file_contents = '\n\n\n' + file_contents[positions[i]:positions[i+1]]  # add some newlines at the beginning
        # read the contract from the header:
        match = re.search(contract_number_regex, new_file_contents)
        
        new_identifier = match.group(1) + '_' + tag
        
        if new_identifier in new_identifiers:
            print(f"Found duplicate new identifier when parsing: {identifier}")
            continue
        
        new_identifiers.append(new_identifier)
        splits.append((new_identifier, new_file_contents))

    return splits


class Contract:
    def __init__(self, filepath_or_identifier: Path | str, contract_type = ContractType.TYPE1) -> None:
        if isinstance(filepath_or_identifier, str):
            identifier = filepath_or_identifier
            filepath = contract_type_paths[contract_type.name] / (identifier + '.txt')
        else:
            filepath = filepath_or_identifier
        
        self.filepath = filepath
        self.identifier = filepath.stem
        _, self.tag = self.identifier.split('_')
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

    def write_to_disk(self):
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
    
    def extract(self):
        if self.NARROW_REGEX:
            matches = self.preprocess(self.NARROW_REGEX)
        else:
            matches = [self.file_contents]
            
        processed_lines = []
        for match in matches:
            rows = self._parse(match, self.identifier)
            processed_lines.extend(rows)
    
        self.rows = processed_lines
        self._df = pd.DataFrame(self.rows)
        
        if self.rows and 'postprocess' in self.__class__.__dict__:  # this checks if postprocess is implemented in the class
            self._df = self.postprocess(self._df)

    @staticmethod
    def postprocess(df):
        """
        Optional postprocessing of extracted DataFrame.
        """
        raise NotImplementedError


class Info(ContractPortionBase):
    
    COLUMNS = [IDENTIFIER, POSTPONED_CONTRACT, NUMBER_OF_BIDDERS, BID_OPENING_DATE, 
               CONTRACT_DATE, CONTRACT_NUMBER, TOTAL_NUMBER_OF_WORKING_DAYS, CONTRACT_ITEMS, 
               CONTRACT_DESCRIPTION, PERCENT_OVER_EST, PERCENT_UNDER_EST, ENGINEERS_EST, 
               AMOUNT_OVER, AMOUNT_UNDER, CONTRACT_CODE]
        
    # narrow from the beginning of the file to the first occurrence of BID RANK or POSTPONED CONTRACT
    NARROW_REGEX = r'(?s)(^.*?(?:Bid Rank|Postponed Contract))'  # TODO find postponed contract in the example file
    
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
        row[POSTPONED_CONTRACT] = int(bool(_extract(r"(Postponed Contract)")))
        row[BID_OPENING_DATE] = _extract(r"Bid Opening Date:\s+(\d+\/\d+\/\d+)")
        row[CONTRACT_NUMBER], row[CONTRACT_DATE] = _extract(r"Contract Number:\s*([\w-]+)\s+(\d+\/\d+\/\d+)", (1, 2))
        
        if row[CONTRACT_NUMBER] != identifier[:len(row[CONTRACT_NUMBER])]:
            raise ValueError(f'Contract number {row[CONTRACT_NUMBER]} does not match the identifier {identifier}')
        
        row[CONTRACT_CODE] = _extract(r"Contract Code:(.+)").strip()
        row[CONTRACT_ITEMS] = _extract(r"Number of Items:\s*(\d+)")
        row[TOTAL_NUMBER_OF_WORKING_DAYS] = _extract(r"Total Number of Working Days: \s*(\d+)")
        row[NUMBER_OF_BIDDERS] = _extract(r"Number of Bidders:\s*(\d+)")
        row[ENGINEERS_EST] = _extract(r"Engineers Est:\s*([\d,]+\.\d{2})")
        row[AMOUNT_OVER_UNDER] = _extract(r"Overrun\/Underrun:\s*(-?[\d,]+\.\d{2})")
        row[PERCENT_OVER_UNDER_EST] = _extract(r"Over\/Under Est:\s*(-?[\d,]+\.\d{2})\%")
        row[CONTRACT_DESCRIPTION] = _extract(r"(.+)Number of Items:").strip()
        processed_lines = [row]
        return processed_lines


class Bids(ContractPortionBase):
    
    NARROW_REGEX = r"(?s)Bid Rank\s+Bid Total\s+Bidder Id\s+Bidder Information \(Name\/Address\/Location\)(.*?)(?=Contract  Proposal  of  Low  Bidder)"
    
    @staticmethod
    def _parse(text, identifier):
        """
        Parses a table from a text line by line, works even if BIDER NAME has 3 rows (very rare). Just having a regex pattern does not work since columns are very variable in width, and we need
        extra logic, hence, we parse line by line in 3 steps.
        """
        bids_pattern = BIDS_FIRST_LINE_PATTERN
        lines = text.split('\n')
        
        i = 0
        
        n = len(lines)
        processed_lines = []
        while i < n:
            match = re.match(bids_pattern, lines[i])
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
                i = get_next_line(i, lines)
                
                name_starts = match.start(5)
                name_ends = match.end(5)
                delta = name_ends - name_starts
                match_cslb_line = re.search(r"CSLB#\s*(\w+)(.*)?", lines[i])
                second_line = False
                row[HAS_THIRD_ROW] = 0
                
                while not match_cslb_line:
                    # this should be the case if second line is present
                    if second_line:
                        # this is now third line:
                        row[HAS_THIRD_ROW] = 1
                    match_extra_name = re.match(rf"(?m)^.{{{name_starts}}}(.+)$", lines[i])
                    row[BIDDER_NAME] += ' ' + match_extra_name.group(1)  # this is the second/third etc. line of the bidder name
                    second_line = True
                    i = get_next_line(i, lines)
                    match_cslb_line = re.search(r"CSLB#\s*(\w+)(.*)?", lines[i])
                else:
                    # there is no second line, extract CSLB number
                    row[CSLB_NUMBER] = match_cslb_line.group(1)
                    row[CONTRACT_NOTES] = match_cslb_line.group(2).strip()
                
                i = get_next_line(i, lines)
                        
                # if there is A) then let's also keep moving until we find the "A+B)" (or "A+ADD)") line
                if row[A_PLUS_B_INDICATOR]:
                    while i < n:
                        match_a_plus_b = re.match(r".*(?:A\+B\)|A\+ADD\))\s+\$([\d,]+\.\d{2})", lines[i])
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
    
    NARROW_REGEX = r"(?sm)^([^\S\r\n]*BIDDER\s+ID\s+NAME\s+AND\s+ADDRESS\s+(?:LICENSE\s+NUMBER)?\s+DESCRIPTION\s+OF\s+PORTION\s+OF\s+WORK\s+SUBCONTRACTED)(.*?)(?=[^\S\r\n]*BIDDER\s+ID\s+NAME\s+AND\s+ADDRESS\s+(?:LICENSE\s+NUMBER)?\s+DESCRIPTION\s+OF\s+PORTION\s+OF\s+WORK\s+SUBCONTRACTED|\f|CONTINUED\s+ON\s+NEXT\s+PAGE)"
    
    @staticmethod
    def _parse(header_and_text, identifier):
        """
        This is different then other _parse methods (in other classes), as far as we store header and text and not just text. We use header then to extract the column start positions.
        """
        
        header, text = header_and_text
        r = re.match(SUBCONTRACTORS_FIRST_LINE_REGEX, header)
        lines = text.split('\n')
        
        start0 = r.start(1)
        delta1 = r.start(2) - r.start(1)
        delta2 = r.start(4) - r.start(2)  # see testing/data_type_2/test_subcontractors_input.txt for some long names
        i = 0
        processed_lines = []
        while i < len(lines):
            line = lines[i]
            match = re.match(rf"^.{{{start0}}}(.{{{delta1}}})(.{{{delta2}}})(.+)$", line)
            if match:
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                
                row[BIDDER_ID] = match.group(1).strip()
                row[SUBCONTRACTOR_NAME] = match.group(2).strip()
                row[SUBCONTRACTED_LINE_ITEM] = match.group(3)
                
                if not any([x in row[SUBCONTRACTED_LINE_ITEM] for x in ("PER BID ITEM", "WORK AS DESCRIBED BY BID ITEM(S) LISTED")]):
                    # attempt parsing
                    matches3 = re.search(r"^.*?(?:ITEMS|ITEM NUMBERS|ITEM\(S\):|ITEM)(.*?)(?:\((.+)\))?$", row[SUBCONTRACTED_LINE_ITEM])
                    if matches3:  # gets two groups, for example: ' 6 THRU 8 AND 13 THRU 15 ', '10%'
                        row[ITEM_NUMBERS] = matches3.group(1).replace('THRU', '-').replace('AND', ',').replace('&', ',')
                        if matches3.group(2):
                            row[PERCENT] = matches3.group(2).replace("%", "")
                
                i = get_next_line(i, lines)
                line = lines[i]
                    
                if r.group(3) is not None:  # there is a LICENSE NUMBER column
                    row[SUBCONTRACTOR_LICENSE_NUMBER] = line[r.start(3):r.start(4)].strip()
                else:
                    row[SUBCONTRACTOR_LICENSE_NUMBER] = ''
                row[SUBCONTRACTED_LINE_ITEM] += ' ' + line[r.start(4):].strip()
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
    
    COLUMNS = [ITEM_NUMBER, ITEM_FLAG, ITEM_CODE, ITEM_DESCRIPTION, EXTRA2, ITEM_DOLLAR_AMOUNT]
    
    NARROW_REGEX = r"(?s)Contract\s+Proposal\s+of\s+Low\s+Bidder(.*?)(?=Contract\s+Proposal\s+of\s+Low\s+Bidder|\f|CONTINUED\s+ON\s+NEXT\s+PAGE)"
    
    @staticmethod
    def _parse(text: str, identifier: str):
        """
        Parses a table from a text line by line.
        """
        
        lines = text.split('\n')
        
        i =  get_next_line(0, lines)
        
        header = lines[i]
        match = re.match(r'.*(Unit).*(Amount)', header)
        start_unit = match.start(1)
        start_amount = match.start(2)
        delta_amount_to_unit = start_amount - start_unit

        i =  get_next_line(i, lines)
        
        n = len(lines)
        processed_lines = []
        row = None
        first_line = False
        while i < n:
            line = lines[i]
            match1 = re.match(r'^\s*(\d+)\s+(?:(F|SF|S))?\s*(\d+)\s+(.+)', line)  # collects until ITEM DESCRIPTION
            if match1:
                # this mean we hit the first line, lets parse it and save it
                # but first we need to save any previous line to precessed_lines
                if row:
                    processed_lines.append(row)
                
                start_item_description = match1.start(4)
                # collects Item Description, Extra, Item Dollar Amount
                match2 = re.match(rf'^.{{{start_item_description}}}(.{{{start_unit - start_item_description}}}).{{{delta_amount_to_unit}}}(.*)$', line)
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                row[ITEM_NUMBER] = match1.group(1)
                row[ITEM_FLAG] = match1.group(2)
                row[ITEM_CODE] = match1.group(3)
                row[ITEM_DESCRIPTION] = match2.group(1).strip()
                row[ITEM_DOLLAR_AMOUNT] = match2.group(2)
                first_line = True
            elif row and first_line:
                # this means we might have a second line, lets parse it 
                if len(line) > start_unit:
                    line = line[start_item_description:start_unit]  # we don't need any extra text
                row[ITEM_DESCRIPTION] += ' ' + line.strip()
                first_line = False
            i = get_next_line(i, lines)
        
        # save the last line
        if row:
            processed_lines.append(row)  
        return processed_lines