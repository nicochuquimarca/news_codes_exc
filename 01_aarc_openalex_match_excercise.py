                                # Research in the Media
# Objectives: 1. Match the private data ids (AARC) with publicly available data ids (OpenAlex)
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-02-06: First version
# version 0.2: 2025-02-07: First merge (Faculty and Dictionary)
# version 1.2: 2025-02-10: Handle special cases and create a Economic Faculty List with OpenAlexIds on the Institutions they work and attended school
# version 2.1: 2025-02-11: Get the authors openalex ids and names
# version 2.2: 2025-02-12: Get the authors openalex ids and names (continuation)
# version 3.1: 2025-02-12: Start an doi-author_name match exercise
# version 3.2: 2025-02-15: Continue with the doi-author_name match exercise(handle the duplicates)
# version 3.3: 2025-02-17: Continue with the doi-author_name match exercise (do the match for one author papers)
# version 3.4: 2025-02-18: Continue with the doi-author_name match exercise but generalize the loop and further handle OpenAlexId duplicates
# version 4.1: 2025-02-19: Send the functions to a source code file and perform the doi-author_name excercise iteratively within a function
# version 4.2: 2025-02-20: Get the authors openalex information to handle duplicated OpenAlex authors.
# version 5.1: 2025-02-21: Get the author basic information to do an OpenAlex duplicates excercise
# version 6.1: 2025-02-23: Handle the duplicates and concatenate the manually searched OpenAlex authors
# version 6.2: 2025-02-25: Create a global match between the AARC and OpenAlex authors
# From version 6.3 the source code is also called by the 02_aarc_twitter_match_via_openalex.py file
# version 6.4: 2025-03-06: Build an BusinessEcon Faculty List dictionary to be able to do the Twitter match
# version 6.5: 2025-03-10: Continue building the BusinessEcon Faculty dictionary
# version 6.6: 2025-03-27: Continue with the BusinessEcon Faculty dictionary, but translate the merge to a function
# version 7.1: 2025-03-31: See how many people from the DOI files were not matched by the first exercise
# version 8.1: 2025-04-02: Get the OpenAlex information for the papers (title,date keywords, fields).
# version 8.2: 2025-04-04: Continue with the OpenAlex information for the papers (title,date keywords, fields).
# version 8.3: 2025-04-05: Continue with the OpenAlex information for the papers (title,date keywords, fields).
# version 8.4: 2025-04-13: Handle the duplicates from the second excercise of names matching
# version 8.5: 2025-04-14: Continue with the duplicates from the second excercise of names matching, move to functions for efficiency
# version 9.1: 2025-04-15: Format the OpenAlex information for the papers (title,date keywords, fields).

# Pseudo Code
# 0. Packages, Set Working directory and call the source code
# 1. Get the OpenAlex PaperId and AuthorId from a list of papers using the doi code 
# 2. Doi-author_name match first exercise
# 3. Query the authors with duplicated PersonIds in aarc to get the number of works in their careers
# 4. Produce a first version of the dictionary for the reduced sample (the fn handles OpenAlexId duplicates and concatenates the manually searched OpenAlex authors)
# 5. Produce the 'final_data_usa_OpenAlexIds.csv' and 'final_data_nonusa_OpenAlexIds.csv' files
# 6. Do the match following Bernhard's matching procedure
# 7. Produce the 'BusinessEconFaculty' file with the OpenAlexIds of the faculty members (handle OpenAlexId duplicates and concatenate the manually searched OpenAlex authors)
# 8. Measure the progress of the match between the AARC and OpenAlex authors
# 9. Get other relevant information at the paper level (title, date, keywords, fields)

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
# pd.set_option('display.max_columns', None) # Display all the columns when printing a DataFrame in the terminal
m_path = wd_path + "\\news_codes_exc\\source_code\\" 
sys.path.insert(0, m_path)
import aarc_openalex_scode as aarc_oa

# 1. Get the OpenAlex PaperId and AuthorId from a list of papers using the doi code 
# Time per batch: 178 seconds, 2 minutes and 58 seconds --> approx 3 minutes (1000 papers per batch in parallel) 
# Time to run all the 234 batches: 12 hours and 42 minutes
    # 1.1 Generate the papers to call
papers_to_call   = aarc_oa.gen_papers_doi_to_call(wd_path,source='aarc',scrap_fn = 'paper_authors') # Generate the papers to call
papers_doi_batch = aarc_oa.generate_id_batches(df = papers_to_call, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 8  # Set the number of batches to call the API (1 batch = 1000 papers)
    # 1.2 Call the API to get the authors openalex ids and names
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        doi_vec = papers_doi_batch[i] # Define the authors vector
        # Linear Process (used for debug)
        # paper_authors_df = aarc_oa.linear_paper_scraper(wd_path,doi_vec,'paper_authors')
        # Parallel Process (used to call the API efficiently)
        paper_authors_df = aarc_oa.parallel_paper_scraper(wd_path,doi_vec,'paper_authors')
    # 1.2 Save the final dataframe
z = aarc_oa.gen_final_papers_csv(wd_path = wd_path,scrap_fn = 'paper_authors') # This function generates the 'doi_papers_authors_openalex.csv' file

# 2. Doi-author_name match first exercise
author_inter_dict_df = aarc_oa.doi_author_surname_match_excercise(wd_path = wd_path)

# 3. Query the authors with duplicated PersonIds in aarc to get the number of works in their careers
authors_works_df  = aarc_oa.gen_authors_ids_to_call(wd_path = wd_path,dictionary_type = "aarc_openalex_authors_bernhard_procedure" ) # Get the authors ids to call the API
authors_ids_batch = aarc_oa.generate_id_batches(df = authors_works_df, batch_size = 100) # Transform the DataFrame into batches
num_batches = 3    # Set the number of batches to call the API (1 batch = 100 authors)
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        authors_vec = authors_ids_batch[i] # Define the authors vector
        # Linear Process (used for debug and call the API in this case)
        authors_work = aarc_oa.linear_works_by_year(wd_path = wd_path,authors_vec = authors_vec)
# Save the final dataframe
x = aarc_oa.gen_final_works_by_year_csv(wd_path = wd_path)

# 4. Produce a first version of the dictionary for the reduced sample (the fn handles OpenAlexId duplicates and concatenates the manually searched OpenAlex authors)
author_dictionary  = aarc_oa.gen_aarc_openalex_dictionary(wd_path = wd_path, dictionary_type = "reduced sample")

# 5. Produce the 'final_data_usa_OpenAlexIds.csv' and 'final_data_nonusa_OpenAlexIds.csv' files
x = aarc_oa.gen_final_data_with_oa_ids(wd_path = wd_path, file_name = 'final_data_usa')
y = aarc_oa.gen_final_data_with_oa_ids(wd_path = wd_path, file_name = 'final_data_nonusa')

# 6. Do the match following Bernhard's matching procedure
bernhard_matches = aarc_oa.bernhard_matching_procedure(wd_path) # Does all the bernhard matches, it takes around 2 hours to run
matches_df       = aarc_oa.format_bernhard_matches(wd_path) # Format the matches to be able to merge them with the AARC data

# 7. Produce the 'BusinessEconFaculty' file with the OpenAlexIds of the faculty members (handle OpenAlexId duplicates and concatenate the manually searched OpenAlex authors)
author_businessecon_dictionary  = aarc_oa.gen_aarc_openalex_dictionary(wd_path = wd_path, dictionary_type = "full sample")

# 8. Measure the progress of the match between the AARC and OpenAlex authors
match_df, match_summary_df = aarc_oa.get_aarc_openalex_dictionary_progress(wd_path = wd_path) 

# 9. Get other relevant information at the paper level (title, date, keywords, fields)
#Linear Scraper performance: 445 seconds
#Parallel Scraper performance: 120 seconds, The Parallel scraper is 2.71 times faster.
    # 9.1 Generate the papers to call
papers_to_call   = aarc_oa.gen_papers_doi_to_call(wd_path,source='aarc_yusuf',scrap_fn = 'paper_info') # Generate the papers to call
papers_doi_batch = aarc_oa.generate_id_batches(df = papers_to_call, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 48   # Set the number of batches to call the API (1 batch = 1000 papers)
    # 9.2 Call the API to get the authors openalex ids and names
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        doi_vec = papers_doi_batch[i] # Define the authors vector
        # Linear Process (used for debug)
        # paper_info_df = aarc_oa.linear_paper_scraper(wd_path,doi_vec,'paper_info')
        # Parallel Process (used to call the API efficiently)
        paper_info_df = aarc_oa.parallel_paper_scraper(wd_path,doi_vec,'paper_info')
    # 9.3 Save the final dataframe
z = aarc_oa.gen_final_papers_csv(wd_path = wd_path,scrap_fn = 'paper_info')