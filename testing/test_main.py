import contract
import contract_type2
from pathlib import Path
import pandas as pd
import numpy as np
import pytest
from constants import ERROR
import re


NA_VALUES = [None, "None", '', 'N/A', np.nan, 'nan']
TEST_DATA = Path('testing/data')


def read_test_file(portion_name, contract_type):
    with open(TEST_DATA / f'test_{portion_name}_type{contract_type}_input.txt') as f:
        raw = f.read()
    return raw


def compare(processed_lines, portion_name: str, contract_type: str, mock=False) -> bool:
    """ 
    When saving result use: mock=True, careful to always remove this flag when testing!
    """
    df = pd.DataFrame(processed_lines).astype(str).replace(to_replace=NA_VALUES, value=pd.NA)
    filepath = TEST_DATA / f'test_{portion_name}_type{contract_type}_output.csv'
    if mock:
        save_result(processed_lines, filepath)
    assert_against = pd.read_csv(str(filepath), dtype=str).replace(to_replace=NA_VALUES, value=pd.NA)
    return df.equals(assert_against)


def save_result(processed_lines, path):
    pd.DataFrame(processed_lines).to_csv(path, index=False)

    
def test_info_type1():
    raw = read_test_file('info', 1)
    processed_lines = contract.Info._parse(raw, "01-AAAAAA_1234")    
    assert compare(processed_lines, 'info', 1)


def test_info_type2():
    raw = read_test_file('info', 2)
    matches = re.findall(r'(?s)Bid Summary(.*?)Bid Rank', raw)
    result = [contract_type2.Info2._parse(match, 'test')[0] for match in matches]
    assert compare(result, 'info', 2)


def test_bids_type1():
    raw = read_test_file('bids', 1)
    processed_lines = contract.Bids._parse(raw, "test")    
    assert compare(processed_lines, 'bids', 1)
    
    
def test_bids_type2():
    raw = read_test_file('bids', 2)
    processed_lines = contract_type2.Bids2._parse(raw, "test")   
    assert compare(processed_lines, 'bids', 2)
    
    
def test_subcontractors_type1():
    raw = read_test_file('subcontractors', 1)
    sc = contract.Subcontractors(raw, "test")
    sc.extract()

    assert compare(sc.df, 'subcontractors', 1)


def test_subcontractors_type2():
    raw = read_test_file('subcontractors', 2)
    sc = contract_type2.Subcontractors2(raw, "test")
    sc.extract()
    assert compare(sc.df, 'subcontractors', 2)


def test_items_type1():
    raw = read_test_file('items', 1)
    processed_lines = contract.Items._parse(raw, "test")
    assert compare(processed_lines, 'items', 1)


def test_items_type2():
    raw = read_test_file('items', 2)
    items = contract_type2.Items2(raw, "test")
    items.extract()
    assert compare(items.df, 'items', 2)
    
        
def test_catch_bad_contract_name():
    c = contract_type2.Contract2('type2/02-1J1704_12603')
    with pytest.raises(ValueError):
        c.extract()
    
    
# TODO
# def test_catch_if_portion_cannot_be_extracted():
#     c = contract_type2.Contract('<some contract that is missing bids>')
#     c.extract()
#     assert c.bids.rows == []
#     assert c.bids.df[ERROR][0] == 1