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



load_dotenv()
RAW_DATA_PATH = Path(os.getenv('RAW_DATA_PATH'))
if not RAW_DATA_PATH.exists():
    RAW_DATA_PATH = Path('./RR Procurement - Raw Data')
    if not RAW_DATA_PATH.exists():
        raise ValueError('Make sure to set a path to raw data in the .env file or copy data into root of the repo')
# print(f'Current RAW_DATA_PATH is {RAW_DATA_PATH}')



# class Contract(object):


RESULTS_PATH = RAW_DATA_PATH.parent / 'results'
RESULTS_PATH.mkdir(exist_ok=True, parents=True)

OUTLIERS_PATH = RESULTS_PATH / 'outliers'

RAW_DATA_PATH_PDF = RAW_DATA_PATH / 'PDFs'
OUTLIERS_PATH_PDF = OUTLIERS_PATH / 'PDFs'
OUTLIERS_PATH_PDF.mkdir(exist_ok=True, parents=True)

RAW_DATA_PATH_LINEPRINTER = RAW_DATA_PATH / 'Txt files - lineprinter'
OUTLIERS_PATH_LINEPRINTER = OUTLIERS_PATH / 'Txt files - lineprinter'
OUTLIERS_PATH_LINEPRINTER.mkdir(exist_ok=True, parents=True)

RAW_DATA_PATH_TABLE = RAW_DATA_PATH / 'Txt files - table'
OUTLIERS_PATH_TABLE = OUTLIERS_PATH / 'Txt files - table'
OUTLIERS_PATH_TABLE.mkdir(exist_ok=True, parents=True)

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

BID_RANK = "Bid_Rank"
A_PLUS_B_INDICATOR = "A_plus_B_indicator"
BID_TOTAL = "Bid_Total"   
BIDDER_ID = "Bidder_ID"
BIDDER_NAME = "Bidder_Name"
CSLB_NUMBER = "CSLB_Number"

SUBCONTRACTOR_NAME = "Subcontractor_Name"
SUBCONTRACTED_LINE_ITEM = "Subcontracted_Line_Item"

ITEM_NUMBER = "Item_Number"
ITEM_CODE = "Item_Code"
ITEM_DESCRIPTION = "Item_Description"
ITEM_DOLLAR_AMOUNT = "Item_Dollar_Amount"

CITY = "City"
SUBCONTRACTOR_LICENSE_NUMBER = "Subcontractor_License_Number"

COULD_NOT_PARSE = "COULD NOT PARSE"



def get_contract_number_and_tag_from_filename(filename:str) -> Tuple[str, str]:
    pattern = re.compile(r"^(\d{2}-\w+)\.pdf_(\d+)$", re.IGNORECASE)  # IGNORECASE is critical since names might have both PDF and pdf
    match = pattern.search(filename)
    contract_number, tag = match.groups()
    identifier = f"{contract_number}_{tag}"
    return contract_number, tag, identifier


def get_contract_number(file_contents):
    return extract(file_contents, r"CONTRACT NUMBER\s+([A-Za-z0-9-]+)")


def get_dates(file_contents):
    match = re.search(r"BID OPENING DATE\s+(\d+\/\d+\/\d+).+\s+(\d+\/\d+\/\d+)", file_contents)
    return match.group(1), match.group(2)
    

def extract(file_contents, regex):
    # Search for the pattern in the text
    match = re.search(regex, file_contents)

    if match:
        # Extract first capture group
        return match.group(1)
    else:
        return ""
    

def extract_contract_data(file_contents, identifier):
    row = defaultdict(str)
    row[IDENTIFIER] = identifier
    match = extract(file_contents, r"(POSTPONED CONTRACT)")
    row[POSTPONED_CONTRACT] = 1 if match else 0
    row[BID_OPENING_DATE], row[CONTRACT_DATE] = get_dates(file_contents)
    row[CONTRACT_CODE] = extract(file_contents, r"CONTRACT CODE\s+'([^']+)'")  # check
    row[CONTRACT_ITEMS] = extract(file_contents, r"(\d+)\s+CONTRACT ITEMS")
    row[TOTAL_NUMBER_OF_WORKING_DAYS] = extract(file_contents, r"TOTAL NUMBER OF WORKING DAYS\s+(\d+)")
    row[NUMBER_OF_BIDDERS] = extract(file_contents, r"NUMBER OF BIDDERS\s+(\d+)")
    row[ENGINEERS_EST] = extract(file_contents, r"ENGINEERS EST\s+([\d,]+\.\d{2})")
    row[AMOUNT_OVER] = extract(file_contents, r"AMOUNT OVER\s+([\d,]+\.\d{2})")
    row[AMOUNT_UNDER] = extract(file_contents, r"AMOUNT UNDER\s+([\d,]+\.\d{2})")
    row[PERCENT_OVER_EST] = extract(file_contents, r"PERCENT OVER EST\s+(\d+.\d{2})")
    row[PERCENT_UNDER_EST] = extract(file_contents, r"PERCENT UNDER EST\s+(\d+.\d{2})")
    row[CONTRACT_DESCRIPTION] = extract(file_contents, r"(?:\n)?(.*?)FEDERAL AID").strip()

    return row


def extract_contract_bid_data(file_contents, identifier):
    
    # have fixed width for name (37 characters) and CSLB number (8 digits)
    pattern = re.compile(r"(\d+)\s+(A\))?\s+([\d,]+\.\d{2})\s+(\d+)\s+(.{37})\s+(\d{3} \d{3}-\d{4})(.*)?$\s+(.*?)(.{37})\s+(\d{8})", re.MULTILINE)
    matches = pattern.findall(file_contents)
    
    contract_bid_data = []

    for match in matches:
        row = defaultdict(str)
        row[IDENTIFIER] = identifier
        row[BID_RANK] = match[0]
        row[A_PLUS_B_INDICATOR] = 1 if match[1] else 0
        row[BID_TOTAL] = match[2]
        row[BIDDER_ID] = match[3].strip()
        row[BIDDER_NAME] = match[4].strip()
        row["Bidder_Phone"] = match[5].strip()
        row["Extra"] = match[6]
        row['Weird_Contract_Notes'] = match[7]
        row[BIDDER_NAME] += ' ' + match[8]
        row[BIDDER_NAME] = row[BIDDER_NAME].strip()
        row[CSLB_NUMBER] = match[9] 
        contract_bid_data.append(row)


    # if contract has A+B we need to correct the BID_TOTAL:
    pattern = re.compile(r"A\+B\)\s+([\d,]+\.\d{2})", re.MULTILINE)  # this will find many A+B) matches but it is reasonable to expect that first A+B) matches are all we need
    a_plus_b_bids = pattern.findall(file_contents)
    if a_plus_b_bids:
        for i, a_plus_b_bid in zip(range(len(contract_bid_data)), a_plus_b_bids):  # this does truncation of a_plus_b_bids list 
            contract_bid_data[i][BID_TOTAL] = a_plus_b_bid

    return contract_bid_data


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

    pattern1= re.compile(r"(?s)BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED(.*?)(?=BIDDER ID NAME AND ADDRESS\s+LICENSE NUMBER\s+DESCRIPTION OF PORTION OF WORK SUBCONTRACTED|\f|CONTINUED ON NEXT PAGE)")
    matches1 = pattern1.findall(file_contents)
    if not matches1:
        return []
            
    bid_subcontractor_data = []
    for match1 in matches1:
        pattern2 = re.compile(r"(?m)^\s+(\d{2})?\s+(.{58})\s+(.+)\n\s+(.{38})?(.+)")
        
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


def extract_contract_line_item_data(file_contents, identifier):

    pattern = re.compile(r"(?m)^\s+(\d+)\s+(\(F\))?\s+(\d+)\s+(.{45})\s+(.{35})\s+([\d,]+\.\d{2})(?:\n\s{26}(.+)\n)?")

    matches = pattern.findall(file_contents)

    contract_line_item_data = []
    for match in matches:
        row = defaultdict(str)
        row[IDENTIFIER] = identifier
        row[ITEM_NUMBER] = match[0]
        row["Extra"] = match[1]
        row[ITEM_CODE] = match[2]
        row[ITEM_DESCRIPTION] = match[3].strip() + ' ' + match[6]
        row[ITEM_DOLLAR_AMOUNT] = match[5]
        contract_line_item_data.append(row)
        
    contract_line_item_data
    return contract_line_item_data

def fill_gaps_in_bidder_id(df):
    df[BIDDER_ID] = df[BIDDER_ID].replace('', np.nan)
    df[BIDDER_ID] = df[BIDDER_ID].ffill()
    return df

def write_to_results(df: pd.DataFrame | List, name: str, timestamp=None):
    if isinstance(df, list):
        df = pd.DataFrame(df)
    
    if timestamp:
        df.to_csv(RESULTS_PATH / f'{timestamp}_{name}.csv', index=False)
    else:
        df.to_csv(RESULTS_PATH / f'{name}.csv', index=False)
    

def read_file(filepath: str):
    # Open the file in read mode ('r')
    with open(filepath, 'r') as file:
        # Read the contents of the file into a string
        file_contents = file.read()
    return file_contents


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

