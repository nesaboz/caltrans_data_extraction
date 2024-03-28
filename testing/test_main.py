from contract import *


NA_VALUES = [None, "None", '', 'N/A', np.nan, 'nan']
TEST_DATA = Path('testing/data')


def assert_against(processed_lines, filename):
    df = pd.DataFrame(processed_lines).astype(str).replace(to_replace=NA_VALUES, value=pd.NA)
    assert_against = pd.read_csv(TEST_DATA / filename, dtype=str).replace(to_replace=NA_VALUES, value=pd.NA)
    assert df.equals(assert_against)


def save_result_to_csv_as_output(processed_lines, filename):
    pd.DataFrame(processed_lines).to_csv(TEST_DATA / filename, index=False)


def test_info():
    with open(TEST_DATA / 'test_info_input.txt') as f:
        raw = f.read()
        
    processed_lines = Info._parse(raw, "01-0A0904_1234")
    
    assert_against(processed_lines, 'test_info_output.csv')
    
    
def test_bids():
    with open(TEST_DATA / 'test_bids_input.txt') as f:
        raw = f.read()
        
    processed_lines = Bids._parse(raw, "test")
    
    # save_result_to_csv_as_output(processed_lines, 'test_bid_data_output.csv')
    assert_against(processed_lines, 'test_bids_output.csv')
    
    
def test_subcontractors():
    with open(TEST_DATA / 'test_subcontractors_input.txt') as f:
        raw = f.read()
        
    sc = Subcontractors(raw, "test")
    sc.extract()
    
    # save_result_to_csv_as_output(sc.rows, 'test_subcontractor_data_output.csv')
    assert_against(sc.rows, 'test_subcontractors_output.csv')


def test_items():
    
    with open(TEST_DATA / 'test_items_input.txt') as f:
        raw = f.read()

    processed_lines = Items._parse(raw, "test")
    
    assert_against(processed_lines, 'test_items_output.csv')


def test_29():
    contract = Contract('07-117074_406')
    contract.items.extract()
    assert list(contract.items.df.loc[123:132][EXTRA1]) == ['F', 'S', 'S', 'S', 'S', 'S', 'SF', 'SF', 'SF', 'SF']
