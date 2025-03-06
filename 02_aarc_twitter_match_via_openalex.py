#                                 # Research in the Media
# Objectives: 1. Match tweeter authors (xxx) with private data ids (AARC) via OpenAlex
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-03-04: First version, get a quick look at the twitter data
# All prior versions of the source code were developed for the 01_aarc_openalex_match_excercise.py file
# version 6.2: 2025-03-04: Send functions to the source code file, clean the twitter data


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


