from typing import List, Tuple
from collections import defaultdict
import re
import pandas as pd
import re
import numpy as np
import os
import shutil
from dotenv import load_dotenv
from constants import *


def read_file(filepath: str):
    # must use this encoding to avoid errors
    with open(filepath, 'r', encoding='ISO-8859-1') as file:
        return file.read()



def split_contract(identifier, file_contents, tag) -> List[Tuple[str, str]]:
    """
    Uses phrase in the header to split the contract into multiple partial_texts. If contract_number + tag is non-original, code skips at reports an issue.
    
    Returns a list of tuples with new identifier, new_file_contents.
    """
    split_pattern = re.compile(r'[^\n]*STATE OF CALIFORNIA\s+B I D   S U M M A R Y\s+DEPARTMENT OF TRANSPORTATION')
    contract_number_regex = re.compile(r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")

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
    def __init__(self, relative_filepath: str) -> None:
        """
        Relative_filepath, for example: 'type1/<identifier>.txt' or 'type2/<identifier>.txt'
        """
        self.contract_type, filename = os.path.split(relative_filepath)
        if '.txt' in filename:
            filename = filename.replace('.txt', '')
            
        
        self.filepath = RAW_DATA_PATH / self.contract_type / (filename + '.txt')
        self.identifier = self.filepath.stem
        _, self.tag = self.identifier.split('_')
        self._file_contents = read_file(self.filepath)
        
        if self.contract_type in {'type1', 'type3'}:
            self.info = Info(self.file_contents, self.identifier)
            self.bids = Bids(self.file_contents, self.identifier)
            self.subcontractors = Subcontractors(self.file_contents, self.identifier)
            self.items = Items(self.file_contents, self.identifier)
        elif self.contract_type == 'type2':
            self.info = Info2(self.file_contents, self.identifier)
            self.bids = Bids2(self.file_contents, self.identifier)
            self.subcontractors = Subcontractors2(self.file_contents, self.identifier)
            self.items = Items2(self.file_contents, self.identifier)
        else:
            raise ValueError(f"Contract type {self.contract_type} is not supported")
        
    def extract(self):
        self.info.extract()
        if not self.info.rows:
            raise ValueError(f"Failed to extract basic info for {os.path.join(self.contract_type, self.identifier)}")
        elif int(self.info.rows[0][POSTPONED_CONTRACT]) == 0:
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
        
        if self._df.empty:
            d = {x: '' for x in self.COLUMNS}
            d[IDENTIFIER] = self.identifier
            d[ERROR] = 1
            self._df = pd.DataFrame([d])
            # or raise an error:
            # raise ValueError(f"Failed to extracted info for {self.__class__.__name__} from {self.identifier}")
        
        if 'postprocess' in self.__class__.__dict__:  # this checks if postprocess is implemented in the class
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
               AMOUNT_OVER, AMOUNT_UNDER, CONTRACT_CODE, ERROR]
        
    # narrow from the beginning of the file to the first occurrence of BID RANK or POSTPONED CONTRACT
    NARROW_REGEX = r'(?s)(^.*?(?:BID RANK|POSTPONED CONTRACT))'
    
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
        a = _extract(r"(POSTPONED CONTRACT)")
        row[POSTPONED_CONTRACT] = int(bool(a))
        row[BID_OPENING_DATE], row[CONTRACT_DATE] = _extract(r"BID OPENING DATE\s+(\d+\/\d+\/\d+).+\s+(\d+\/\d+\/\d+)", (1, 2))
        row[CONTRACT_NUMBER] = _extract(r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")
        if row[CONTRACT_NUMBER] != identifier[:len(row[CONTRACT_NUMBER])]:
            raise ValueError(f'Contract number {row[CONTRACT_NUMBER]} does not match the identifier {identifier}')
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
    BIDS_FIRST_LINE_PATTERN = re.compile(r"^\s+(\d+)\s+(A\))?\s+([\d,]+\.\d{2})\s+(\d+)\s+(.+)(\d{3} \d{3}-\d{4})(.*)?")
    
    COLUMNS = [IDENTIFIER, BID_RANK, A_PLUS_B_INDICATOR, BID_TOTAL, BIDDER_ID, 
               BIDDER_NAME, BIDDER_PHONE, EXTRA, CSLB_NUMBER, HAS_THIRD_ROW, CONTRACT_NOTES, ERROR]
    
    @staticmethod
    def _parse(text, identifier):
        """
        Parses a table from a text line by line, works even if BIDER NAME has 3 rows (very rare). Just having a regex pattern does not work since columns are very variable in width, and we need
        extra logic, hence, we parse line by line in 3 steps.
        """
        bids_pattern = Bids.BIDS_FIRST_LINE_PATTERN
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
                i += 1
                name_starts = match.start(5)
                name_ends = match.end(5)
                delta = name_ends - name_starts
                match_second_line = re.match(rf"^(.{{{name_starts}}})(.{{{delta}}})(.+)$", lines[i])
                
                if match_second_line:
                    # this should be the case if second line is present
                    row[CONTRACT_NOTES] = match_second_line.group(1).strip()
                    row[BIDDER_NAME] += ' ' + match_second_line.group(2).rstrip()  # this is the second line of the bidder name
                    row[CSLB_NUMBER] = match_second_line.group(3)
                else:
                    # log as error:
                    raise ValueError(f'Second line is not in the standard format (notes, extra name, CLBS number, line: `{lines[i]}`')
                
                # moving onto the next line, here it might be a third line or an address followed by a FAX number:
                i += 1
                match_fax_number = re.match(r"^.*(FAX).*(?:\d{3} \d{3}-\d{4}).*$", lines[i])  # TODO do we need to capture text after FAX number?
                if not match_fax_number:
                    # this means we have a third line, so just strip and add to the name
                    match_third_line = re.match(rf"^.{{{name_starts}}}(.+)$", lines[i])
                    row[BIDDER_NAME] += match_third_line.group(1).strip()
                    row[HAS_THIRD_ROW] = 1
                else:
                    # we don't have a third row
                    row[HAS_THIRD_ROW] = 0
                    # if there is A) then let's also keep moving until we find the "A+B)" (or "A+ADD)") line
                    if row[A_PLUS_B_INDICATOR]:
                        while i < n:
                            match_a_plus_b = re.match(r".*(?:A\+B\)|A\+ADD\))\s+([\d,]+\.\d{2}).*", lines[i])
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
    
    COLUMNS = [IDENTIFIER, BIDDER_ID, SUBCONTRACTOR_NAME, SUBCONTRACTED_LINE_ITEM, CITY, SUBCONTRACTOR_LICENSE_NUMBER, ERROR]
    
    NARROW_REGEX = r"(?sm)^([^\S\r\n]*BIDDER ID\s+NAME AND ADDRESS\s+(?:LICENSE NUMBER)?\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED)(.*?)(?=[^\S\r\n]*BIDDER ID NAME AND ADDRESS\s+(?:LICENSE NUMBER)?\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED|\f|CONTINUED\s+ON\s+NEXT\s+PAGE)"
    
    @staticmethod
    def _parse(header_and_text, identifier):
        """
        This is different then other _parse methods (in other classes), as far as we store header and text and not just text. We use header then to extract the column start positions.
        """
        
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
                
                # alternate approach so just check if you get the same
                if match.group(1):
                    assert row[BIDDER_ID] == match.group(1), f"{row[BIDDER_ID]} is not {match.group(1)}"
                if match.group(2).strip():
                    assert row[SUBCONTRACTOR_NAME] == match.group(2).strip(), f"{row[SUBCONTRACTOR_NAME]} is not {match.group(2).strip()}"
                if match.group(3).strip():
                    assert row[SUBCONTRACTED_LINE_ITEM] == match.group(3).strip(), f"{row[SUBCONTRACTED_LINE_ITEM]} is not {match.group(3).strip()}"
                
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
    
    COLUMNS = [ITEM_NUMBER, ITEM_FLAG, ITEM_CODE, ITEM_DESCRIPTION, EXTRA2, ITEM_DOLLAR_AMOUNT, ERROR]
    
    NARROW_REGEX = r"(?s)C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R(.*?)(?=C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R|\f|CONTINUED ON NEXT PAGE)"
    
    @staticmethod
    def _parse(text: str, identifier: str):
        """
        Parses a table from a text line by line.
        """
        
        lines = text.split('\n')
        
        i = 0
        
        n = len(lines)
        processed_lines = []
        row = None
        first_line = False
        while i < n:
            line = lines[i]
            match = re.match(r'^\s+(\d+)\s+(?:\((F|SF|S)\))?\s*(\d+)\s+(.{45})(.*)\s+([\d,]+\.\d{2})', line)
            # re.compile(r'^\s+(\d+)\s+(?:\((F|SF|S)\))?\s*(\d+)\s+(.{45})\s+(.*)\s+([\d,]+\.\d{2})')
            if match:
                # this mean we hit the first line, lets parse it and save it
                # but first we need to save any previous line to precessed_lines
                if row:
                    processed_lines.append(row)
                
                row = defaultdict(str)
                row[IDENTIFIER] = identifier
                row[ITEM_NUMBER] = match.group(1)
                row[ITEM_FLAG] = match.group(2)
                row[ITEM_CODE] = match.group(3)
                row[ITEM_DESCRIPTION] = match.group(4).strip()

                row[ITEM_DOLLAR_AMOUNT] = match.group(6)
                first_line = True
            elif row and first_line:
                # this means we have a second line, let's append it to the ITEM_DESCRIPTION
                row[ITEM_DESCRIPTION] += line.strip()
                first_line = False
            i += 1
        
        # save the last line
        if row:
            processed_lines.append(row)  
        return processed_lines
    
    

def get_next_line(i, lines):
    i += 1
    n = len(lines)
    while i < n and lines[i].strip() == '':
        i += 1
    return i
                            

class Info2(ContractPortionBase):
    
    COLUMNS = [IDENTIFIER, POSTPONED_CONTRACT, NUMBER_OF_BIDDERS, BID_OPENING_DATE, 
               CONTRACT_DATE, CONTRACT_NUMBER, TOTAL_NUMBER_OF_WORKING_DAYS, CONTRACT_ITEMS, 
               CONTRACT_DESCRIPTION, PERCENT_OVER_EST, PERCENT_UNDER_EST, ENGINEERS_EST, 
               AMOUNT_OVER, AMOUNT_UNDER, CONTRACT_CODE, ERROR]
        
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
        
        # if identifier == 'test' then do not check this
        if identifier != 'test' and row[CONTRACT_NUMBER] != identifier[:len(row[CONTRACT_NUMBER])]:
            raise ValueError(f'Contract number {row[CONTRACT_NUMBER]} does not match the identifier {identifier}')
        
        row[CONTRACT_CODE] = _extract(r"Contract Code:(.+)").strip()
        row[CONTRACT_ITEMS] = _extract(r"Number of Items:\s*(\d+)")
        row[TOTAL_NUMBER_OF_WORKING_DAYS] = _extract(r"Total Number of Working Days: \s*(\d+)")
        row[NUMBER_OF_BIDDERS] = _extract(r"Number of Bidders:\s*(\d+)")
        row[ENGINEERS_EST] = _extract(r"Engineers Est:\s*([\d,]+\.\d{2})")
        row[AMOUNT_OVER_UNDER] = _extract(r"Overrun\/Underrun:\s*(-?[\d,]+\.\d{2})")
        row[PERCENT_OVER_UNDER_EST] = _extract(r"Over\/Under Est:\s*(-?[\d,]+\.\d{2})\%")
        contract_description, next_line = _extract(r"(.+)Number of Items:\s+\d+\n\s*(.*)\n", (1,2))
        row[CONTRACT_DESCRIPTION] = contract_description.strip().rsplit("  ")[-1]  # this basically picks up everything before "Number of Items" until it hits 2 spaces
        if 'Federal Aid' not in next_line:
            # this is then a second line of the description
            row[CONTRACT_DESCRIPTION] += ' ' + next_line.strip() 
        
        processed_lines = [row]
        return processed_lines


class Bids2(ContractPortionBase):
    
    NARROW_REGEX = r"(?s)Bid\s+Rank\s+Bid\s+Total\s+Bidder\s+Id\s+Bidder\s+Information\s+\(Name\/Address\/Location\)(.*?)(?=Contract\s+Proposal\s+of\s+Low\s+Bidder)"
    
    BIDS_FIRST_LINE_PATTERN = re.compile(r"^(\d+)\s+(A\))?\s+\$([\d,]+\.\d{2})\s*(\w+)\s*(.+)Phone\s*(\(\d{3}\)\d{3}-\d{4})(.*)?")
    
    COLUMNS = [IDENTIFIER, BID_RANK, A_PLUS_B_INDICATOR, BID_TOTAL, BIDDER_ID, 
               BIDDER_NAME, BIDDER_PHONE, EXTRA, CSLB_NUMBER, HAS_THIRD_ROW, CONTRACT_NOTES, ERROR]
    
    @staticmethod
    def _parse(text, identifier):
        """
        Parses a table from a text line by line, works even if BIDER NAME has 3 rows (very rare). Just having a regex pattern does not work since columns are very variable in width, and we need
        extra logic, hence, we parse line by line in 3 steps.
        """
        bids_pattern = Bids2.BIDS_FIRST_LINE_PATTERN
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


class Subcontractors2(ContractPortionBase):
    
    COLUMNS = [IDENTIFIER, BIDDER_ID, SUBCONTRACTOR_NAME, SUBCONTRACTED_LINE_ITEM, CITY, SUBCONTRACTOR_LICENSE_NUMBER, ERROR]
    SUBCONTRACTORS_FIRST_LINE_REGEX = r"[^\S\r\n]*(BIDDER\s+ID)\s+(NAME\s+AND\s+ADDRESS)\s+(LICENSE\s+NUMBER)?\s+(DESCRIPTION\s+OF\s+PORTION\s+OF\s+WORK\s+SUBCONTRACTED)"
    
    NARROW_REGEX = r"(?sm)^([^\S\r\n]*BIDDER\s+ID\s+NAME\s+AND\s+ADDRESS\s+(?:LICENSE\s+NUMBER)?\s+DESCRIPTION\s+OF\s+PORTION\s+OF\s+WORK\s+SUBCONTRACTED)(.*?)(?=[^\S\r\n]*BIDDER\s+ID\s+NAME\s+AND\s+ADDRESS\s+(?:LICENSE\s+NUMBER)?\s+DESCRIPTION\s+OF\s+PORTION\s+OF\s+WORK\s+SUBCONTRACTED|\f|CONTINUED\s+ON\s+NEXT\s+PAGE)"
    
    @staticmethod
    def _parse(header_and_text, identifier):
        """
        This is different then other _parse methods (in other classes), as far as we store header and text and not just text. We use header then to extract the column start positions.
        """
        
        header, text = header_and_text
        r = re.match(Subcontractors2.SUBCONTRACTORS_FIRST_LINE_REGEX, header)
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


class Items2(ContractPortionBase):
    
    COLUMNS = [ITEM_NUMBER, ITEM_FLAG, ITEM_CODE, ITEM_DESCRIPTION, EXTRA2, ITEM_DOLLAR_AMOUNT, ERROR]
    
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
    