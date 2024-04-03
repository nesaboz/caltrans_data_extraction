import os
from pathlib import Path
from dotenv import load_dotenv

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
SUBCONTRACTOR_LICENSE_NUMBER_PRE = "Subcontractor_License_Number_Pre"
SUBCONTRACTOR_LICENSE_NUMBER_POST = "Subcontractor_License_Number_Post"
WRONG_INDENTATION = "Wrong_Indentation"

ERROR_FILENAME = "Error_Filename"
ERROR = "Error"


# Check if the code is running in GitHub Actions
if os.environ.get('GITHUB_ACTIONS') != 'true':
    # This code block will only execute locally
    load_dotenv()
    RAW_DATA_PATH = Path(os.getenv('RAW_DATA_PATH'))
    if not RAW_DATA_PATH.exists():
        raise ValueError('Make sure to set a path to raw data in the .env file.')
    
    LINEPRINTER_LABEL = 'lineprinter'
    TABLE_LABEL = 'table'
    DOC = 'doc'

    RAW_DATA_PATH_LINEPRINTER = RAW_DATA_PATH / LINEPRINTER_LABEL
    RAW_DATA_PATH_TABLE = RAW_DATA_PATH / TABLE_LABEL
    RAW_DATA_PATH_DOC = RAW_DATA_PATH / DOC

    PROCESSED_DATA_PATH = RAW_DATA_PATH / 'processed'

    RESULTS_PATH = Path('results')
