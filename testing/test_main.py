from contract import *


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
        
    processed_lines = Info._parse(raw, "test")
    
    assert_against(processed_lines, 'test_contract_data_output.csv')
    
    
def test_bid_data():
    with open(TEST_DATA / 'test_bid_data_input.txt') as f:
        raw = f.read()
        
    processed_lines = Bids._parse(raw, "test")
    
    # save_result_to_csv_as_output(processed_lines, 'test_bid_data_output.csv')
    assert_against(processed_lines, 'test_bid_data_output.csv')
    
    
def test_subcontractor_data():
    with open(TEST_DATA / 'test_subcontractor_data_input.txt') as f:
        raw = f.read()
        
    sc = Subcontractors(raw, "test")
    sc.extract()

    assert_against(sc.rows, 'test_subcontractor_data_output.csv')


def test_line_item_data():
    
    with open(TEST_DATA / 'test_line_item_data_input.txt') as f:
        raw = f.read()

    processed_lines = Items._parse(raw, "test")
    assert_against(processed_lines, 'test_line_item_data_output.csv')


def test_29():
    contract = Contract('07-117074_406')
    contract.items.extract()
    assert list(contract.items.df.loc[123:132][EXTRA1]) == ['F', 'S', 'S', 'S', 'S', 'S', 'SF', 'SF', 'SF', 'SF']

# def test_write_to_excel():
#     # contract = Contract('07-117074_406')
#     contract = Contract('01-0A0904_2724')
#     contract.extract()
#     contract.write_to_excel()
