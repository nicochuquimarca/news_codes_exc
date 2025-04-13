# 01_tempfile
# 0. Packages
import sys, os, pandas as pd, ast, requests, math # Import the regular packages
from datetime import date                         # Get the current date
from datetime import datetime                     # Get the current date and time
import concurrent.futures, string                 # For parallel processing
import glob, time                                 # To call elements in a folder and time measurement
from unidecode import unidecode                   # For string manipulation
import numpy as np                                # For numerical operations

# 0. Set Working directory and call the source code
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)                          # Set the working directory
pd.set_option('display.max_columns', None) # Display all the columns when printing a DataFrame in the terminal
m_path = wd_path + "\\news_codes_exc\\source_code\\" 
sys.path.insert(0, m_path)
import aarc_openalex_scode as aarc_oa

# EXPERIMENTAL CODE
### PENDING JOB: WORK WITH THE RAW DATAFRAME TO MAKE A COMPREHENSIVE DICTIONARY DATAFRAME
# THE FINAL COLUMNS SHOULD BE
# These are the four columns I want to have in the final df
# PersonId,PersonName,PersonOpenAlexId,PersonOpenAlexName
# The name of the file should be aarc_openalex_authors_matches_bernhard_procedure.csv
# Include the dupplicates in this file, put them in a new row
# 24161: {'A5082611670', 'A5001633041'}

# 1. Read the CSV file and change the name of the OpenAlex column (I put a wrong name in the previous code)
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_berhnard_raw_file\\bernhard_procedure_author_match.csv"
author_matches_df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
author_matches_df = author_matches_df.rename(columns={'PersonOpenAlexName': 'PersonOpenAlexId'}) # Rename columns

# 2. Transform the 'PersonOpenAlexId' column from string to multiple row values within the same column
# 2.1 Ensure the 'PersonOpenAlexId' column is parsed as sets
author_matches_df['PersonOpenAlexId'] = author_matches_df['PersonOpenAlexId'].apply(
    lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('{') else x
)
# 2.2 Convert the 'PersonOpenAlexId' column from sets to lists
author_matches_df['PersonOpenAlexId'] = author_matches_df['PersonOpenAlexId'].apply(lambda x: list(x) if isinstance(x, set) else x)
#2.3 Use the explode method to create a new row for each element in 'PersonOpenAlexId'
author_matches_df = author_matches_df.explode('PersonOpenAlexId', ignore_index=True)
# 3. Add the 'PersonName' column from the AARC dataset
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
aarc_df = pd.read_excel(file_path, sheet_name='Sheet1')
# 3.1 Select columns, remove duplicates and rename columns
aarc_df = aarc_df[['aarc_personid', 'aarc_name']] # Select columns
aarc_df = aarc_df.drop_duplicates() # Remove duplicates
aarc_df = aarc_df.rename(columns={'aarc_personid': 'PersonId', 'aarc_name': 'PersonName'}) # Rename columns
# 3.2 Merge the two dataframes on 'PersonId' and do a quick check of the result
author_matches_dfm = pd.merge(author_matches_df, aarc_df, on='PersonId', how='left')
aarc_oa.check_duplicates_and_missing_values(author_matches_df, author_matches_dfm, 'PersonName', True)
# 4. Add the 'PersonOpenAlexName' column from the AARC dataset
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
ao_df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
# 4.1 Select columns, remove duplicates and rename columns
ao_df = ao_df[['author_id', 'author_display_name']] # Select columns
ao_df = ao_df.drop_duplicates() # Remove duplicates
ao_df = ao_df.rename(columns={'author_id': 'PersonOpenAlexId', 'author_display_name': 'PersonOpenAlexName'}) # Rename columns
# 4.2 Merge the two dataframes on 'PersonOpenAlexId' and do a quick check of the result
author_matches_dfmm = pd.merge(author_matches_dfm, ao_df, on='PersonOpenAlexId', how='left')
aarc_oa.check_duplicates_and_missing_values(author_matches_dfm, author_matches_dfmm, 'PersonOpenAlexName', False) # It is put false beacause some values are null by definition
# 5. Reorder the columns for aesthetic purposes
author_matches_dfmm = author_matches_dfmm[['PersonId', 'PersonName', 'PersonOpenAlexId', 'PersonOpenAlexName']]
# 6. Replace 'set()' for None for those unable to be matched and create a column that indicates if the author was matched or not
author_matches_dfmm['matched'] = author_matches_dfmm['PersonOpenAlexName'].notnull()
author_matches_dfmm['PersonOpenAlexId'] = author_matches_dfmm['PersonOpenAlexId'].replace("set()", None)
# 7. Save the dataframes under the names 'aarc_openalex_authors_matches_bernhard_procedure.csv' and 'aarc_openalex_authors_nomatches_bernhard_procedure.csv'
fp01 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_bernhard_procedure.xlsx"
author_matches_dfmm.to_excel(fp01, index=False)
