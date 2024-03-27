# Data Extraction from Contracts

We extract data from 11'000 PDF contracts available publicly for the purposes of research project. Considering well structured and high quality text, extraction was done using regex library in Python. 

![sample contract snapshot](assets/sample.png)

See actual contract in `sample` folder. 

## Quick install

1) Get raw data locally from [Google Drive](https://drive.google.com/drive/folders/1X-8v6XCqYComYpxVVtznc05AA6-G5Tvu?usp=share_link) (email [Maria](mkhrakov@chicagobooth.edu) for access)
2) Edit path in .env file to point to the raw data.
3) Follow main.ipynb to set up the environment and perform single and buld contract parsing.

## Detailed setup (optional)

1) Make sure Python is installed on the system you are using (likely anything 3.8+ will do it).

2) [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) git repository (you will need access since this is a private repository), the link to the repository is:
`https://github.com/nesaboz/caltrans_data_extraction`

3) (optional) create a virtual environment or conda environment:
```bash
<path_to_python> -m venv <env_name>
source <env_name>/bin/activate
```

4) Run the following command in the terminal/cmd to install required packages:
```bash
pip install pandas==2.2.1 numpy==1.26.4 tqdm==4.66.2 ipykernel==6.29.3 notebook==7.1.1 python-dotenv==1.0.1 openpyxl==3.1.2 pytest==8.1.1 pyperclip==1.8.2
```
or, install the libraries using the requirements.txt file:
```bash
pip install -r requirements.txt
```

5) Add jupyter kernel to the virtual environment:
```bash 
python -m ipykernel install --user --name=<env_name>
```

6) Open jupyter notebook in your IDE (like VSCode) or run `jupyter notebook` in terminal.

7) Set up kernel to the one you just created in the previous step.
![](assets/kernel.jpg)

7) Check hard-coded path in the main.ipynb pointing to raw data, or edit path in `.env` for `RAW_DATA_PATH`.
