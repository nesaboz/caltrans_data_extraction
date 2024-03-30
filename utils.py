from typing import Tuple
import re

parse_filename_pattern = re.compile(r"^(\d{2}-\w+)\.pdf_(\d+)$", re.IGNORECASE)  # IGNORECASE is critical since names might have both PDF and pdf
split_pattern = re.compile(r'[^\n]*STATE OF CALIFORNIA\s+B I D   S U M M A R Y\s+DEPARTMENT OF TRANSPORTATION')


def parse_filename(filename:str) -> Tuple[str, str]:
    match = parse_filename_pattern.search(filename)
    contract_number, tag = match.groups()
    identifier = f"{contract_number}_{tag}"
    return contract_number, tag, identifier


def read_file(filepath: str):
    # must use this encoding to avoid errors
    with open(filepath, 'r', encoding='ISO-8859-1') as file:
        return file.read()