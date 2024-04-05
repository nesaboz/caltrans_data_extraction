# Data Extraction from Contracts

We extract data from 11'000+ text files that contain California Transportation contracts available publicly for the purposes of a research project. Considering well structured and high quality text, extraction was done using regex library in Python. 
I relied heavily on [regex101.com](https://regex101.com) as a useful tool for quick prototyping and testing. There are many regex tutorials available online.

![sample contract snapshot](assets/sample.png)

## Colab

Go to this <a href="https://githubtocolab.com/nesaboz/caltrans_data_extraction/blob/main/main.ipynb/" target="_blank">link</a>.

## Local (quick install)

1) Get raw data.zip from [Google Drive](https://drive.google.com/file/d/1y-ufhK56J3h994I5HKiarcFzbCkB6h_h/view?usp=share_link)
2) Unzip and make sure the structure is as follows (all names are hard-coded in the script so if you are changing them, make sure you change them in the code as well):
```text
- data
    - lineprinter
    - table
    - doc
```
3) Follow main.ipynb to set up the environment.

## Local (detailed setup)

1) Make sure Python 3.11.4 is installed on the system you are using (likely anything 3.8+ will do it but still 3.11.4 will work for sure).

2) [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) git repository (you will need access since this is a private repository), the link to the repository is:
`https://github.com/nesaboz/caltrans_data_extraction`

3) (optional) create a virtual environment or conda environment:
```bash
<path_to_python> -m venv <env_name>
source <env_name>/bin/activate
```

4) Run the following command in the terminal/cmd to install required packages:
```bash
pip install pandas==2.2.1 numpy==1.26.4 tqdm==4.66.2 ipykernel==6.29.3 notebook==7.1.1 openpyxl==3.1.2 pytest==8.1.1 pyperclip==1.8.2
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

