#                                 # Research in the Media
# Objectives: 1. Match tweeter authors (xxx) with private data ids (AARC) via OpenAlex
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-03-04: First version


# Pseudo Code
# 0. Packages, and Set Working directory 
# 1. Load the twitter data

# 0. Packages
import sys, os, pandas as pd
# 0. Set Working directory and call the source code
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)                          # Set the working directory
pd.set_option('display.max_columns', None) # Display all the columns when printing a DataFrame in the terminal

# 1. Load the twitter data
fp = wd_path + "\\data\\raw\\twitter_openalex\\input_files\\authors_tweeters_2024_02.csv"
twitter_df = pd.read_csv(fp)
twitter_df['OpenAlexId'] = twitter_df['author_id'].apply(lambda x: x.split('/')[-1]) # Extract the openalex id
twitter_df
# Do a duplicates check of multiple twitter ids with the same openalex id and viceversa

