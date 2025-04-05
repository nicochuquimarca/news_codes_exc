#                                 # Research in the Media
# Objectives: 1. Match tweeter authors (xxx) with private data ids (AARC) via OpenAlex
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-03-04: First version, get a quick look at the twitter data
# All prior versions of the source code were developed for the 01_aarc_openalex_match_excercise.py file
# version 6.4: 2025-03-06: Send functions to the source code file, clean the twitter data
# version 6.5: 2025-03-10: Continue building the BusinessEcon Faculty dictionary
# version 6.6: 2025-03-10: Make a first matching with the twitter data
# version 6.7: 2025-03-18: Make a match with the twitterid duplicates data

# Pseudo Code
# 0. Packages, and Set Working directory 
# 1. Prepare the twitter data for the twitter-openalex-aarc matching (1:1 matching)


# 0. Packages
import sys, os, pandas as pd, ast, requests, math # Import the regular packages
from datetime import date                         # Get the current date
from datetime import datetime                     # Get the current date and time
import concurrent.futures, string                 # For parallel processing
import glob, time                                 # To call elements in a folder and time measurement
from unidecode import unidecode                   # For string manipulation

# 0. Set Working directory and call the source code
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)                          # Set the working directory
pd.set_option('display.max_columns', None) # Display all the columns when printing a DataFrame in the terminal
m_path = wd_path + "\\news_codes_exc\\source_code\\" 
sys.path.insert(0, m_path)
import aarc_openalex_scode as aarc_oa

# 1. Prepare the twitter data for the twitter-openalex-aarc matching (1:1 matching)
twitter_df =  aarc_oa.prepare_twitter_data(wd_path)

# The final product is to knowth whether the authors have twitter or not, not to make an exact match. Therefore, you just need 
# to 
# 1.1 Prepare
# twitter_df.columns --> 'TwitterId', 'OpenAlexLink', 'PersonOpenAlexId', 'MatchingMethod'

# GENERAL STEPS: 1. OPEN THE FUZZY TWITTER DATA: WORK WITH DUPLICATES TWITTER IDS...MAKE THE MERGE, THEN REMOVE DUPLICATES AND THEN MERGE WITH THE 1:1

# Open the file
file_path = wd_path + "\\data\\raw\\twitter_openalex\\input_files\\fuzzy_twitter_dfs\\authors_tweeters_2024_02_duplicated_twitter_ids.csv"
tw_dup_df = pd.read_csv(file_path)
# Count the duplicates for OpenAlexId
tw_dup_df['OpenAlexId_dups'] = tw_dup_df['OpenAlexId'].map(tw_dup_df['OpenAlexId'].value_counts()) # Count the duplicates
# Filter the df by their duplicates
df1 = tw_dup_df[tw_dup_df['OpenAlexId_dups'] == 1]  # Get the duplicates
df2 = tw_dup_df[tw_dup_df['OpenAlexId_dups'] > 1]  # Get the duplicates
# Remove the TwitterId duplicates from df1
df1 = df1.drop_duplicates(subset = 'TwitterId', keep = 'first')
df1 = df1.drop(columns=['TwitterId_dups','OpenAlexId_dups'])
# Change the name of a variable before the concatenation
df1 = df1.rename(columns={'OpenAlexId': 'PersonOpenAlexId'})
# Concatenate the df1 with the twitter_df
fp = wd_path + "\\data\\raw\\twitter_openalex\\input_files\\authors_tweeters_2024_02_clean.csv"
tw_df = pd.read_csv(fp)
twitter_df = pd.concat([tw_df, df1], axis = 0)
twitter_df = twitter_df.drop_duplicates()

# 2. Open the business_econ faculty dictionary
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_businessecon_dictionary.xlsx"
be_df = pd.read_excel(file_path)

# 3. Match the twitter data with the business_econ faculty dictionary
be_twitter_df = pd.merge(be_df, twitter_df, how = "left", on = "PersonOpenAlexId")
be_twitter_match_df = be_twitter_df[be_twitter_df["TwitterId"].notnull()]
file_path = wd_path + "\\data\\raw\\twitter_openalex\\output_files\\aarc_openalex_twitter_author_businessecon_dictionary.xlsx"
be_twitter_match_df.to_excel(file_path, index = False)

