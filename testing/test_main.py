from utils import *
from io import StringIO

NA_VALUES = [None, "None", '', 'N/A', np.nan, 'nan']
TEST_DATA = Path('testing/data')

def assert_against(processed_lines, filename):
    df = pd.DataFrame(processed_lines).astype(str).replace(to_replace=NA_VALUES, value=pd.NA)
    assert_against = pd.read_csv(TEST_DATA / filename, dtype=str).replace(to_replace=NA_VALUES, value=pd.NA)
    assert df.equals(assert_against)


def save_result_to_csv_as_output(processed_lines, filename):
    pd.DataFrame(processed_lines).to_csv(TEST_DATA / filename, index=False)


def test_contract_data():
    with open(TEST_DATA / 'test_contract_data_input.txt') as f:
        raw = f.read()
        
    processed_lines = ContractData._parse(raw, "test")
    
    assert_against(processed_lines, 'test_contract_data_output.csv')
    
    
def test_bid_data():
    with open(TEST_DATA / 'test_bid_data_input.txt') as f:
        raw = f.read()
        
    processed_lines = BidData._parse(raw, "test")
    
    assert_against(processed_lines, 'test_bid_data_output.csv')
    
    
def test_subcontractor_data():
    with open(TEST_DATA / 'test_subcontractor_data_input.txt') as f:
        raw = f.read()
        
    processed_lines = SubcontractorData._parse(raw, "test")
    
    assert_against(processed_lines, 'test_subcontractor_data_output.csv')


def test_line_item_data():
    
    with open(TEST_DATA / 'test_line_item_data_input.txt') as f:
        raw = f.read()

    processed_lines = LineItemData._parse(raw, "test")
    assert_against(processed_lines, 'test_line_item_data_output.csv')

                
def test_29():
    contract = Contract('07-117074_406')
    lid = LineItemData(contract)
    lid.extract()
    assert list(lid.df.loc[123:132][EXTRA1]) == ['F', 'S', 'S', 'S', 'S', 'S', 'SF', 'SF', 'SF', 'SF']

def test_write_to_excel():
    import pandas as pd

    # Assuming you have two dataframes df1 and df2
    df1 = pd.DataFrame({'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']})
    df2 = pd.DataFrame({'Column3': [4, 5, 6], 'Column4': ['D', 'E', 'F']})

    # Create a Pandas Excel writer using openpyxl as the engine
    with pd.ExcelWriter('test.xlsx', engine='openpyxl') as writer:
        # Iterate over your CSV files
        for df, name in zip([df1, df2], ['name1', 'name2']):
            # Write the DataFrame to a new sheet in the Excel file using the file name as the sheet name
            df.to_excel(writer, sheet_name=name, index=False)
