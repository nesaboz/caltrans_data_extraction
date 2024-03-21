from utils import *
from io import StringIO

NA_VALUES = [None, '', 'N/A', np.nan]
TEST_DATA = Path('testing/data')


def test_parse_table():
    
    raw_text = """
                04-4G6404                                                                                                BID211
            04-SM-84-21                C O N T R A C T   P R O P O S A L   O F   L O W   B I D D E R                 PAGE 48
            11/03/15                                                                                                 11/05/15

   ------------------------------------------------------------------------------------------------------------------------------------
       ITEM      ITEM                                                   UNIT OF     ESTIMATED
        NO.      CODE                  ITEM DESCRIPTION                 MEASURE     QUANTITY          BID              AMOUNT
   ------------------------------------------------------------------------------------------------------------------------------------

       106 (F) 839727      CONCRETE BARRIER (TYPE 736 MODIFIED)           LF          454              101.00        45,854.00
       107     840504      4" THERMOPLASTIC TRAFFIC STRIPE                LF        1,240                1.00         1,240.00
       108     840505      6" THERMOPLASTIC TRAFFIC STRIPE                LF          320                1.50           480.00
       109     840506      8" THERMOPLASTIC TRAFFIC STRIPE                LF          530                2.20         1,166.00
       110     840507      6" THERMOPLASTIC TRAFFIC STRIPE                LF          300                1.40           420.00
                           (BROKEN 8-4)
       111     840515      THERMOPLASTIC PAVEMENT MARKING                 SQFT        230                3.30           759.00
       112     840526      4" THERMOPLASTIC TRAFFIC STRIPE                LF          470                1.00           470.00
                           (BROKEN 17-7)
       113     850111      PAVEMENT MARKER (RETROREFLECTIVE)              EA           46                5.00           230.00
       114     860090      MAINTAINING EXISTING TRAFFIC MANAGEMENT        LS     LUMP SUM              500.00           500.00
                           SYSTEM ELEMENTS DURING CONSTRUCTION
       115     860400      LIGHTING (TEMPORARY)                           LS     LUMP SUM           54,000.00        54,000.00
       116     861503      MODIFY LIGHTING                                LS     LUMP SUM           71,000.00        71,000.00
       117     995100      WATER METER CHARGES                            LS     LUMP SUM            6,600.00         6,600.00
       118     995200      IRRIGATION WATER SERVICE CHARGES               LS     LUMP SUM            1,000.00         1,000.00
       119     000003      ITEM DELETED PER ADDENDUM                      LS     LUMP SUM                 .00             0.00
       120     208424      1 1/4" BACKFLOW PREVENTER ASSEMBLY             EA            1            4,000.00         4,000.00
       121     999990      MOBILIZATION                                   LS     LUMP SUM          325,000.00       325,000.00

                                                                                                      A)          3,387,137.00
                                                                                                      B)   90 DAYS X    3000
                                                                                                             -----------------
                                                                                                     A+B)         3,657,137.00



"""


    assert_against = [
            ('106', '(F)', '839727', 'CONCRETE BARRIER (TYPE 736 MODIFIED)         ', 'LF          454              101.00', '45,854.00', ''),
('107', None, '840504', '4" THERMOPLASTIC TRAFFIC STRIPE              ', 'LF        1,240                1.00', '1,240.00', ''),
('108', None, '840505', '6" THERMOPLASTIC TRAFFIC STRIPE              ', 'LF          320                1.50', '480.00', ''),
('109', None, '840506', '8" THERMOPLASTIC TRAFFIC STRIPE              ', 'LF          530                2.20', '1,166.00', ''),
('110', None, '840507', '6" THERMOPLASTIC TRAFFIC STRIPE              ', 'LF          300                1.40', '420.00', ' (BROKEN 8-4)'),
('111', None, '840515', 'THERMOPLASTIC PAVEMENT MARKING               ', 'SQFT        230                3.30', '759.00', ''),
('112', None, '840526', '4" THERMOPLASTIC TRAFFIC STRIPE              ', 'LF          470                1.00', '470.00', ' (BROKEN 17-7)'),
('113', None, '850111', 'PAVEMENT MARKER (RETROREFLECTIVE)            ', 'EA           46                5.00', '230.00', ''),
('114', None, '860090', 'MAINTAINING EXISTING TRAFFIC MANAGEMENT      ', 'LS     LUMP SUM              500.00', '500.00', ' SYSTEM ELEMENTS DURING CONSTRUCTION'),
('115', None, '860400', 'LIGHTING (TEMPORARY)                         ', 'LS     LUMP SUM           54,000.00', '54,000.00', ''),
('116', None, '861503', 'MODIFY LIGHTING                              ', 'LS     LUMP SUM           71,000.00', '71,000.00', ''),
('117', None, '995100', 'WATER METER CHARGES                          ', 'LS     LUMP SUM            6,600.00', '6,600.00', ''),
('118', None, '995200', 'IRRIGATION WATER SERVICE CHARGES             ', 'LS     LUMP SUM            1,000.00', '1,000.00', ''),
('119', None, '000003', 'ITEM DELETED PER ADDENDUM                    ', 'LS     LUMP SUM                 .00', '0.00', ''),
('120', None, '208424', '1 1/4" BACKFLOW PREVENTER ASSEMBLY           ', 'EA            1            4,000.00', '4,000.00', ''),
('121', None, '999990', 'MOBILIZATION                                 ', 'LS     LUMP SUM          325,000.00', '325,000.00', ' ')
]
    # Your original regex pattern
    regex = r'^\s+(\d+)\s+(\(F\))?\s+(\d+)\s+(.{45})\s+(.{35})\s+([\d,]+\.\d{2})'
    regex_tag = r'(.*+)'

    matches = parse_table(
        raw_text,
        regex,
        regex_tag
        )

    for match in matches:
        assert match.groups() == assert_against.pop(0)
    assert assert_against == []
    
    
def test_parse_low_bidder_table():
    filepath = TEST_DATA / '04-4G6404.pdf_7310.txt'
    
    contract_number_from_filename, tag, identifier = get_contract_number_and_tag_from_filename(filepath.stem)
    file_contents = read_file(filepath)
    
    df = pd.DataFrame(extract_contract_line_item_data(file_contents, identifier))
    
    assert_against = pd.read_csv(TEST_DATA / '04-4G6404.pdf_7310_low_bidder_table.csv', dtype=str)
    
    pd.testing.assert_frame_equal(df, assert_against)
    
def test_parse_item_table():
    
    filepath = TEST_DATA / '04-4G6404.pdf_7310.txt'

    contract_number_from_filename, tag, identifier = get_contract_number_and_tag_from_filename(filepath.stem)
    file_contents = read_file(filepath)

    df_bid_subcontractor_data, df_bid_subcontractor_data_outliers = parse_subcontracted_line_item(fill_gaps_in_bidder_id(pd.DataFrame(extract_bid_subcontractor_data(file_contents, identifier))))

    assert_against_data = pd.read_csv(TEST_DATA / '04-4G6404.pdf_7310_subcontractor_data.csv', dtype=str)
    assert_against_data_outliers = pd.read_csv(TEST_DATA / '04-4G6404.pdf_7310_subcontractor_data_outliers.csv', dtype=str)

    df_bid_subcontractor_data = df_bid_subcontractor_data.replace(to_replace=NA_VALUES, value=pd.NA).reset_index(drop=True)
    df_bid_subcontractor_data_outliers = df_bid_subcontractor_data_outliers.replace(to_replace=NA_VALUES, value=pd.NA).reset_index(drop=True)
    
    pd.testing.assert_frame_equal(df_bid_subcontractor_data, assert_against_data)
    pd.testing.assert_frame_equal(df_bid_subcontractor_data_outliers, assert_against_data_outliers)


def test_line_item_parsing():
    df = pd.read_csv(TEST_DATA / 'raw_subcontracted_line_item_examples.txt', header=None, delimiter="\t", names=['Subcontracted_Line_Item'])
    df, df_outlier = parse_subcontracted_line_item(df)
    
    df = df.replace(to_replace=NA_VALUES, value=pd.NA).reset_index(drop=True)
    
    assert_against = pd.read_csv(TEST_DATA / 'assertion_subcontracted_line_item_examples.csv', dtype=str)
    
    pd.testing.assert_frame_equal(df, assert_against)
    
def test_contract_bid():
    raw = """                         4           401,116.00    2              GOLDEN STATE BRIDGE, INC.             925 372-8000  CC PREF CLAIMED
                                                                                                        00851187
                                                                  3701 MALLARD DRIVE;               FAX 925 372-8001
                                                                  BENICIA CA  94510

                         5           247,247.00    3              TRUESDELL CORPORATION OF              602 437-1711  CC PREF CLAIMED
                                 FAILED TO SUBMIT DVBE/DBE QUOTES CALIFORNIA, INC.                      00615058
                                                                  ASDF
                                                                  1310 W 23RD STREET;               FAX 602 437-1821
                                                                  TEMPE AZ  85282
"""

    df = pd.DataFrame(extract_contract_bid_data(raw, '04-3Q5204.pdf_12593')).astype(str)
    df = df.replace(to_replace=NA_VALUES, value=pd.NA)
    
    assert_against = """Identifier,Bid_Rank,A_plus_B_indicator,Bid_Total,Bidder_ID,Bidder_Name,Bidder_Phone,Extra,Weird_Contract_Notes,CSLB_Number,Third_Row
04-3Q5204.pdf_12593,4,0,"401,116.00",2,"GOLDEN STATE BRIDGE, INC.",925 372-8000,  CC PREF CLAIMED,,00851187,0
04-3Q5204.pdf_12593,5,0,"247,247.00",3,"TRUESDELL CORPORATION OF CALIFORNIA, INC. ASDF",602 437-1711,  CC PREF CLAIMED,FAILED TO SUBMIT DVBE/DBE QUOTES,00615058,1"""
    data = StringIO(assert_against)
    df_assert_against = pd.read_csv(data, dtype=str)
    pd.testing.assert_frame_equal(df, df_assert_against)
