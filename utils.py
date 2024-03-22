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

class ContractType(Enum):
    type1 = 1
    type2 = 2
    
    
class CohortType(Enum):
    sample = 1
    population = 2
    
    
def read_file(filepath: str):
    # must use this encoding to avoid errors
    with open(filepath, 'r', encoding='ISO-8859-1') as file:
        return file.read()
    
    
def save_contract_types():
    """
    Inspect quickly contract to determine if it is of type 1 or 2.
    """
    RAW_LINEPRINTER_FILES = Path('data/lineprinter_txt_files')
    files = RAW_LINEPRINTER_FILES.glob('*.txt')

    CONTRACT_TYPE = "Contract_Type"

    contract_types = []
    for i, filepath in enumerate(files):
        filename = filepath.stem
        row = defaultdict(str)
        row["Filename"] = filename
        
        # must use this encoding to avoid errors
        file_contents = read_file(filepath)
            
        match = re.search(r"CONTRACT\s+NUMBER\s+([A-Za-z0-9-]+)", file_contents)
        
        if match:
            row[CONTRACT_TYPE] = 1
        else:
            row[CONTRACT_TYPE] = 2
                    
        contract_types.append(row)
        
    df = pd.DataFrame(contract_types)
    df.set_index('Filename', inplace=True)
    df.to_csv('data/contract_types.csv', index=True)
    

def get_contract_types_dict():
    """Use as:
    d = get_contract_types_dict()
    d['01-1234.pdf_1']  # return 1 or 2
    """
    df = pd.read_csv('data/contract_types.csv')
    df.set_index('Filename', inplace=True)
    return df['Contract_Type'].to_dict()
    

class Contract:
    
    def __init__(self, filename: str, contract_types) -> None:
        self.contract_number, self.tag, self.identifier = self.get_contract_number_and_tag_from_filename(filename)
        
        if contract_types[filename] == 1:
            filepath = Path('data/lineprinter_txt_files') / (filename + '.txt')
        else:
            filepath = Path('data/table_txt_files') / (filename + '.txt')
        
        self._file_contents = read_file(filepath)
            
    def get_contract_number_and_tag_from_filename(self, filename:str) -> Tuple[str, str]:
        pattern = re.compile(r"^(\d{2}-\w+)\.pdf_(\d+)$", re.IGNORECASE)  # IGNORECASE is critical since names might have both PDF and pdf
        match = pattern.search(filename)
        contract_number, tag = match.groups()
        identifier = f"{contract_number}_{tag}"
        return contract_number, tag, identifier
    
    @property
    def file_contents(self):
        return self._file_contents
        
    def narrow_file_contents(self, regex: str) -> List[str]:
        """
        Uses regex to narrow down the file_contents to specific sections that will be returned.
        """
        pattern = re.compile(regex)
        matches = pattern.findall(self.file_contents)
        return matches if matches else []


class ContractData:

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
    
    columns = [IDENTIFIER, POSTPONED_CONTRACT, NUMBER_OF_BIDDERS, BID_OPENING_DATE, 
               CONTRACT_DATE, CONTRACT_NUMBER, TOTAL_NUMBER_OF_WORKING_DAYS, CONTRACT_ITEMS, 
               CONTRACT_DESCRIPTION, PERCENT_OVER_EST, PERCENT_UNDER_EST, ENGINEERS_EST, 
               AMOUNT_OVER, AMOUNT_UNDER, CONTRACT_CODE]
    
    def __init__(self, file_contents: str, identifier: str) -> None:
        self.file_contents = file_contents
        self.identifier = identifier
        
    def extract(self, regex, groups=(1,)):
        # Search for the pattern in the text
        match = re.search(regex, self.file_contents)

        if match:
            if len(groups) == 1:
                return match.group(groups[0])
            else:
                return (match.group(i) for i in groups)
        else:
            return ""
    
    def get_row(self):
        row = defaultdict(str)
        row[self.IDENTIFIER] = self.identifier
        row[self.POSTPONED_CONTRACT] = int(bool(self.extract(r"(POSTPONED CONTRACT)")))
        row[self.BID_OPENING_DATE], row[self.CONTRACT_DATE] = self.extract(r"BID OPENING DATE\s+(\d+\/\d+\/\d+).+\s+(\d+\/\d+\/\d+)", (1, 2))
        row[self.CONTRACT_NUMBER] = self.extract(r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")
        row[self.CONTRACT_CODE] = self.extract(r"CONTRACT CODE\s+'([^']+)'")  # check
        row[self.CONTRACT_ITEMS] = self.extract(r"(\d+)\s+CONTRACT ITEMS")
        row[self.TOTAL_NUMBER_OF_WORKING_DAYS] = self.extract(r"TOTAL NUMBER OF WORKING DAYS\s+(\d+)")
        row[self.NUMBER_OF_BIDDERS] = self.extract(r"NUMBER OF BIDDERS\s+(\d+)")
        row[self.ENGINEERS_EST] = self.extract(r"ENGINEERS EST\s+([\d,]+\.\d{2})")
        row[self.AMOUNT_OVER] = self.extract(r"AMOUNT OVER\s+([\d,]+\.\d{2})")
        row[self.AMOUNT_UNDER] = self.extract(r"AMOUNT UNDER\s+([\d,]+\.\d{2})")
        row[self.PERCENT_OVER_EST] = self.extract(r"PERCENT OVER EST\s+(\d+.\d{2})")
        row[self.PERCENT_UNDER_EST] = self.extract(r"PERCENT UNDER EST\s+(\d+.\d{2})")
        row[self.CONTRACT_DESCRIPTION] = self.extract(r"(?:\n)?(.*?)FEDERAL AID").strip()
        return row


class ContractBidData:
    
    IDENTIFIER = "Identifier"
    BID_RANK = "Bid_Rank"
    A_PLUS_B_INDICATOR = "A_plus_B_indicator"
    BID_TOTAL = "Bid_Total"   
    BIDDER_ID = "Bidder_ID"
    BIDDER_NAME = "Bidder_Name"
    BIDDER_PHONE = "Bidder_Phone"
    EXTRA = "Extra"
    CSLB_NUMBER = "CSLB_Number"
    THIRD_ROW = "Has_Third_Row"
    CONTRACT_NOTES = 'Contract_Notes'

    def __init__(self, file_contents: str, identifier: str) -> None:
        self.file_contents = file_contents
        self.identifier = identifier
        

    # def old_parse_contract_bid_data(file_contents, identifier):
    #     # old method using only regex; fails to parse if BIDER NAME has 3 rows
    #     # To delete unless parse_contract_bid_data starts failing
                
    #     # have fixed width for name (37 characters) and CSLB number (8 digits)
    #     pattern = re.compile(r"(\d+)\s+(A\))?\s+([\d,]+\.\d{2})\s+(\d+)\s+(.{38})\s(\d{3} \d{3}-\d{4})(.*)?$\s+(.*?)(.{38})\s(\d+)$\s+(.+)", re.MULTILINE)
    #     matches = pattern.findall(file_contents)
        
    #     contract_bid_data = []

    #     for match in matches:
    #         row = defaultdict(str)
    #         row[IDENTIFIER] = identifier
    #         row[BID_RANK] = match[0]
    #         row[A_PLUS_B_INDICATOR] = 1 if match[1] else 0
    #         row[BID_TOTAL] = match[2]
    #         row[BIDDER_ID] = match[3].strip()
    #         row[BIDDER_NAME] = match[4].strip()
    #         row["Bidder_Phone"] = match[5].strip()
    #         row["Extra"] = match[6]
    #         row['Weird_Contract_Notes'] = match[7]
    #         row[BIDDER_NAME] += ' ' + match[8]  # this is the second line of the bidder name
    #         row[BIDDER_NAME] = row[BIDDER_NAME].strip()
    #         row[CSLB_NUMBER] = match[9]
    #         row[BIDDER_NAME] += ' ' + match[10]  # this is the third line of the bidder name
    #         row[BIDDER_NAME] = row[BIDDER_NAME].strip()
    #         contract_bid_data.append(row)

    #     return contract_bid_data


    def parse(self):
        """
        Parses a table from a text line by line, works even if BIDER NAME has 3 rows (very rare).
        """
        pattern1 = re.compile(r"^\s+(\d+)\s+(A\))?\s+([\d,]+\.\d{2})\s+(\d+)\s+(.{38})\s(\d{3} \d{3}-\d{4})(.*)?")
        pattern2 = re.compile(r"^(.{65})(.{38})\s+(\d{8})$")
        pattern3 = re.compile(r"^.+(FAX|B\)|\d{3} \d{3}-\d{4}|;).*+$")
        
        lines = self.file_contents.split('\n')

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
                    if self.THIRD_ROW not in row:
                        row[self.THIRD_ROW] = 0
                    processed_lines.append(row)
                
                match = match1.groups()
                row = defaultdict(str)
                row[self.IDENTIFIER] = self.identifier
                row[self.BID_RANK] = match[0]
                row[self.A_PLUS_B_INDICATOR] = 1 if match[1] else 0
                row[self.BID_TOTAL] = match[2]
                row[self.BIDDER_ID] = match[3].strip()
                row[self.BIDDER_NAME] = match[4].strip()
                row[self.BIDDER_PHONE] = match[5].strip()
                row[self.EXTRA] = match[6]
            
                just_passed_first_line = True
                just_passed_second_line = False
                just_passed_third_line = False
                
            elif match2 and just_passed_first_line:
                match = match2.groups()
                
                row[self.CONTRACT_NOTES] = match[0].strip()
                row[self.BIDDER_NAME] += ' ' + match[1].strip()  # this is the second line of the bidder name
                row[self.BIDDER_NAME] = row[self.BIDDER_NAME].strip()
                row[self.CSLB_NUMBER] = match[2]
                
                just_passed_first_line = False
                just_passed_second_line = True
                just_passed_third_line = False
            
            elif match3 is None and just_passed_second_line:
                # this means we are on the third line and it does not have some keyword from pattern3 in it
                # then just add this line to the name
                row[self.BIDDER_NAME] += ' ' + line.strip()
                row[self.BIDDER_NAME] = row[self.BIDDER_NAME].strip()
                row[self.THIRD_ROW] = 1
                just_passed_first_line = False
                just_passed_second_line = False
                just_passed_third_line = True
            else:
                just_passed_first_line = False
                just_passed_second_line = False
                just_passed_third_line = False

        if row:
            if self.THIRD_ROW not in row:
                row[self.THIRD_ROW] = 0
            processed_lines.append(row)   
        
        return processed_lines

    def get_rows(self):
        contract_bid_data = self.parse()

        # if contract has A+B we need to correct the BID_TOTAL:
        # the following will find many A+B) matches but it is reasonable to expect that first A+B) matches are all we need
        pattern = re.compile(r"A\+B\)\s+([\d,]+\.\d{2})", re.MULTILINE)  
        a_plus_b_bids = pattern.findall(self.file_contents)
        if a_plus_b_bids:
            for i, a_plus_b_bid in zip(range(len(contract_bid_data)), a_plus_b_bids):  # this does truncation of a_plus_b_bids list 
                contract_bid_data[i][self.BID_TOTAL] = a_plus_b_bid

        return contract_bid_data


class Extractor:

    def __init__(self, cohort_type: CohortType):
        
        load_dotenv()
        RAW_DATA_PATH = Path(os.getenv('RAW_DATA_PATH'))
        if not RAW_DATA_PATH.exists():
            raise ValueError('Make sure to set a path to raw data in the .env file or copy data into root of the repo')
        
        RAW_DATA_PATH_PDF = RAW_DATA_PATH / f'PDFs - {cohort_type.name}'
        RAW_DATA_PATH_LINEPRINTER = RAW_DATA_PATH / f'Txt files - lineprinter - {cohort_type.name}'
        RAW_DATA_PATH_TABLE = RAW_DATA_PATH / f'Txt files - table - {cohort_type.name}'

        RESULTS_PATH = RAW_DATA_PATH.parent / f'results/{cohort_type.name}'
        OUTLIERS_PATH = RESULTS_PATH / 'outliers'
        OUTLIERS_PATH_PDF = OUTLIERS_PATH / f'PDFs - {cohort_type.name}'
        OUTLIERS_PATH_LINEPRINTER = OUTLIERS_PATH / f'Txt files - lineprinter - {cohort_type.name}'
        OUTLIERS_PATH_TABLE = OUTLIERS_PATH / f'Txt files - table - {cohort_type.name}'

        RESULTS_PATH.mkdir(exist_ok=True, parents=True)
        OUTLIERS_PATH_PDF.mkdir(exist_ok=True, parents=True)
        OUTLIERS_PATH_LINEPRINTER.mkdir(exist_ok=True, parents=True)
        OUTLIERS_PATH_TABLE.mkdir(exist_ok=True, parents=True)
    
    def write_to_results(df: pd.DataFrame | List, name: str, timestamp=None):
        if isinstance(df, list):
            df = pd.DataFrame(df)
        
        if timestamp:
            df.to_csv(RESULTS_PATH / f'{timestamp}_{name}.csv', index=False)
        else:
            df.to_csv(RESULTS_PATH / f'{name}.csv', index=False)
    
    def run_batch(files: List[Path], results_path: Path, add_timestamp=False):
        """
        Run a batch or a single file (by making `files` a single element list).
        """
        if add_timestamp:
            timestamp = datetime.strftime(datetime.now(), '%m-%d-%Y-%H:%M:%S')
        else:
            timestamp = None
        
        contract_data = []
        contract_bid_data = []
        bid_subcontractor_data = []
        contract_line_item_data = []
        other_format = []
        error_files = []

        for filepath in tqdm(files):
            try:
                filename = filepath.stem
                file_contents = read_file(filepath)
                
                contract_number_from_filename, tag, identifier = get_contract_number_and_tag_from_filename(filename)
                contract_number_from_contents = get_contract_number(file_contents)
                
                if contract_number_from_filename == contract_number_from_contents:  
                    contract_data.append(extract_contract_data(file_contents, identifier))
                    contract_bid_data.extend(extract_contract_bid_data(file_contents, identifier))
                    bid_subcontractor_data.extend(extract_bid_subcontractor_data(file_contents, identifier))
                    contract_line_item_data.extend(extract_contract_line_item_data(file_contents, identifier))
                else:
                    # if contract number doesn't match then something is off that needs investigation
                    other_format.append({'other_format_filename': filename})
                    # let's also copy the pdf to a folder for manual inspection
                    
                    shutil.copy(
                        RAW_DATA_PATH_LINEPRINTER / f'{filename}.txt', 
                        OUTLIERS_PATH_LINEPRINTER / f'{filename}.txt'
                        )
                    
                    shutil.copy(
                        RAW_DATA_PATH_TABLE / f'{filename}.txt',
                        OUTLIERS_PATH_TABLE / f'{filename}.txt'
                        )     
                    
                    shutil.copy(
                        RAW_DATA_PATH_PDF / f'{filename}.pdf', 
                        OUTLIERS_PATH_PDF / f'{filename}.pdf'
                        )
            except Exception as e:
                error_files.append({'error_files': filename, 'error': e})

        write_to_results(contract_data, "contract_data", timestamp=timestamp)
        write_to_results(contract_bid_data, "contract_bid_data", timestamp=timestamp)

        df_bid_subcontractor_data, df_bid_subcontractor_data_could_not_parse = parse_subcontracted_line_item(
            fill_gaps_in_bidder_id(pd.DataFrame(bid_subcontractor_data)))

        write_to_results(df_bid_subcontractor_data, "bid_subcontractor_data", timestamp=timestamp)
        write_to_results(df_bid_subcontractor_data_could_not_parse, "bid_subcontractor_outliers", timestamp=timestamp)

        write_to_results(contract_line_item_data, "contract_line_item_data", timestamp=timestamp)
        write_to_results(other_format, "other_format", timestamp=timestamp)

        write_to_results(error_files, "errors", timestamp=timestamp)





class BidSubcontractorData:
    
    SUBCONTRACTOR_NAME = "Subcontractor_Name"
    SUBCONTRACTED_LINE_ITEM = "Subcontracted_Line_Item"
    CITY = "City"
    SUBCONTRACTOR_LICENSE_NUMBER = "Subcontractor_License_Number"

class ContractLineItemData:
    
    ITEM_NUMBER = "Item_Number"
    ITEM_CODE = "Item_Code"
    ITEM_DESCRIPTION = "Item_Description"
    ITEM_DOLLAR_AMOUNT = "Item_Dollar_Amount"
    COULD_NOT_PARSE = "COULD NOT PARSE"


    def extract_bid_subcontractor_data(file_contents, identifier):
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

        matches1 = narrow_file_contents(
            file_contents, 
            r"(?s)BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED(.*?)(?=BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED|\f|CONTINUED ON NEXT PAGE)"
            )
            
        pattern2 = re.compile(r"(?m)^\s+(\d{2})?\s+(.{58})\s+(.+)\n\s+(.{38})?(.+)")
                
        bid_subcontractor_data = []
        for match1 in matches1:
            matches2 = pattern2.findall(match1)
            for match2 in matches2:
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                row[BIDDER_ID] = match2[0]
                row[SUBCONTRACTOR_NAME] = match2[1].strip()
                row[SUBCONTRACTED_LINE_ITEM] = match2[2]
                row[CITY] = match2[3].strip()
                row[SUBCONTRACTOR_LICENSE_NUMBER] = match2[4].strip()
                bid_subcontractor_data.append(row)

        return bid_subcontractor_data


    def parse_table(text, regex, regex_tag):
        """
        Parses a table from a text line by line, but 

        Args:
            text (_type_): _description_
            regex (_type_): _description_
            regex_tag (_type_): _description_

        Returns:
            _type_: _description_
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


    def extract_contract_line_item_data(file_contents, identifier):
        
        matches1 = narrow_file_contents(
            file_contents, 
            r"(?s)C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R(.*?)(?=C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R|\f|CONTINUED ON NEXT PAGE)"
            )
        
        regex = r'^\s+(\d+)\s+(\(F\))?\s+(\d+)\s+(.{45})\s+(.{35})\s+([\d,]+\.\d{2})'
        regex_tag = r'(.*+)'

        contract_line_item_data = []
        for match1 in matches1:
            matches2 = parse_table(match1, regex, regex_tag)
            for match2 in matches2:
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                row[ITEM_NUMBER] = match2[1]
                row["Extra"] = match2[2]
                row[ITEM_CODE] = match2[3]
                row[ITEM_DESCRIPTION] = match2[4].strip() + ' ' + match2[7]
                row['Extra1'] = match2[5]
                row[ITEM_DOLLAR_AMOUNT] = match2[6]
                
                contract_line_item_data.append(row)
                
        return contract_line_item_data

    def expand_ranges_in_subcontracted_line_item(line: str) -> str:
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
            
            # Return a comma-separated string of all_numbers
            return ", ".join(map(str, all_numbers))
        except:
            return COULD_NOT_PARSE
        

    def parse_subcontracted_line_item(df):
        """
        Takes a Subcontracted_Line_Item in df, and splits into three columns: Y1, Y2, Y3.
        For example "SOME TEXT ITEMS 6 THRU 8 AND 13 THRU 15 (PARTIALS)", will be split into:
        - SOME TEXT, 
        - ITEMS, 
        - 6 THRU 8 AND 13 THRU 15, 
        - (PARTIALS)
        Next, the "6 THRU 8 AND 13 THRU 15" will be converted into "6-8, 13-15" and then expanded to "6, 7, 8, 13, 14, 15".
        """
        # splits subcontracted line item into three columns
        df[['PARSED_1', 'PARSED_2', 'PARSED_3', 'PARSED_4']] = df[SUBCONTRACTED_LINE_ITEM].str.extract(r"^(.+?)?(ITEMS|ITEM NUMBERS|ITEM\(S\):|ITEM)(.+?)(\(.+\))?$")
        # replace the 'THRU' and 'AND' with '-' and ','
        df['PARSED_3'] = df['PARSED_3'].str.replace('THRU', '-', regex=False).str.replace('AND', ',', regex=False).str.replace('&', ',', regex=False)
        # extend all the ranges
        df['PARSED_5'] = df['PARSED_3'].apply(expand_ranges_in_subcontracted_line_item)
        df_outliers = df[df['PARSED_5'] == COULD_NOT_PARSE]
        return df, df_outliers


    def fill_gaps_in_bidder_id(df):
        df[BIDDER_ID] = df[BIDDER_ID].replace('', np.nan)
        df[BIDDER_ID] = df[BIDDER_ID].ffill()
        return df
    
    
