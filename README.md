# Data Extraction from Contracts

We extract data from 11'000 PDF contracts available publicly for the purposes of research project. Considering well structured and high quality text, extraction was done using regex library in Python. 

![alt text](image.png)


## Quick install

Get raw data locally from google drive and change the path in main.ipynb to point to it. Run main.ipynb. Follow instructions in the notebook to set up the environment.

## Extra setup (optional)

1) Make sure Python is installed on the system you are using (likely anything 3.8+ will do it).

2) Clone git repository (you might need an access since this is a private repository):
```bash
git clone https://github.com/nesaboz/regex.git
```

3) (optional) create a virtual environment (or conda environment):
```bash
<path_to_python> -m venv <env_name>
source env_name_of_choice/bin/activate
```

4) Run the following command in the terminal/cmd to install required packages:
```bash
pip install pandas numpy tqdm ipykernel notebook
```
or if there are any weird versioning issues just use frozen versions of the libraries:
```bash
pip install pandas==2.2.1 numpy==1.26.4 tqdm==4.66.2 ipykernel==6.29.3 notebook==7.1.1
```
or, install the libraries using the requirements.txt file:
```bash
pip install -r requirements.txt
```

5) (optional) instead of hard-coded path in the main.ipynb, one can also create an environmental variable `RAW_DATA_PATH`. Many ways to do this including adding the following line to your .bashrc or .bash_profile file:
```bash
export RAW_DATA_PATH=<path_to_raw_data>
```
or adding .env file to the root of the project with the following line:
```bash
RAW_DATA_PATH=<path_to_raw_data>
```
