import contract
import contract_type2
from pathlib import Path
import pandas as pd
import numpy as np


NA_VALUES = [None, "None", '', 'N/A', np.nan, 'nan']
TEST_DATA = Path('testing/data')
TEST_DATA_TYPE2 = Path('testing/data_type2')


def assert_against(processed_lines, path):
    df = pd.DataFrame(processed_lines).astype(str).replace(to_replace=NA_VALUES, value=pd.NA)
    assert_against = pd.read_csv(path, dtype=str).replace(to_replace=NA_VALUES, value=pd.NA)
    assert df.equals(assert_against)


def save_result_to_csv_as_output(processed_lines, path):
    pd.DataFrame(processed_lines).to_csv(path, index=False)


def test_info():
    with open(TEST_DATA / 'test_info_input.txt') as f:
        raw = f.read()
        
    processed_lines = contract.Info._parse(raw, "01-0A0904_1234")
    
    assert_against(processed_lines, TEST_DATA / 'test_info_output.csv')
    
    
def test_bids():
    with open(TEST_DATA / 'test_bids_input.txt') as f:
        raw = f.read()
        
    processed_lines = contract.Bids._parse(raw, "test")
    
    assert_against(processed_lines, TEST_DATA / 'test_bids_output.csv')
    
    
def test_subcontractors():
    with open(TEST_DATA / 'test_subcontractors_input.txt') as f:
        raw = f.read()
        
    sc = contract.Subcontractors(raw, "test")
    sc.extract()

    assert_against(sc.rows, TEST_DATA / 'test_subcontractors_output.csv')


def test_items():
    
    with open(TEST_DATA / 'test_items_input.txt') as f:
        raw = f.read()

    processed_lines = contract.Items._parse(raw, "test")
    
    assert_against(processed_lines, TEST_DATA / 'test_items_output.csv')


def test_info2():
    with open(TEST_DATA_TYPE2 / 'test_info_input.txt') as f:
        raw = f.read()
        
    processed_lines = contract_type2.Info._parse(raw, "02-2J1704_1234")
    # save_result_to_csv_as_output(processed_lines, TEST_DATA_TYPE2, 'test_info_output.csv')
    assert_against(processed_lines, TEST_DATA_TYPE2 / 'test_info_output.csv')
    
    
def test_bids2():
    with open(TEST_DATA_TYPE2 / 'test_bids_input.txt') as f:
        raw = f.read()
        
    processed_lines = contract_type2.Bids._parse(raw, "test")
    
    # save_result_to_csv_as_output(processed_lines, 'test_bid_data_output.csv')
    assert_against(processed_lines, TEST_DATA_TYPE2 / 'test_bids_output.csv')
    

    ### TODO ones below
    
    
def test_subcontractors2():
    with open(TEST_DATA_TYPE2 / 'test_subcontractors_input.txt') as f:
        raw = f.read()
        
    sc = contract_type2.Subcontractors(raw, "test")
    sc.extract()
    
    # save_result_to_csv_as_output(sc.rows, 'test_subcontractor_data_output.csv')
    assert_against(sc.rows, TEST_DATA_TYPE2 / 'test_subcontractors_output.csv')


def test_items2():
    
    with open(TEST_DATA_TYPE2 / 'test_items_input.txt') as f:
        raw = f.read()

    processed_lines = contract_type2.Items._parse(raw, "test")
    
    assert_against(processed_lines, TEST_DATA_TYPE2 / 'test_items_output.csv')
