                                # Research in the Media
# Objectives: 1. Match the private data ids (AARC) with publicly available data ids (OpenAlex)
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1:  2025-02-06: First version
# version 0.2:  2025-02-07: First merge (Faculty and Dictionary)
# version 1.2:  2025-02-10: Handle special cases and create a Economic Faculty List with OpenAlexIds on the Institutions they work and attended school
# version 2.1:  2025-02-11: Get the authors openalex ids and names
# version 2.2:  2025-02-12: Get the authors openalex ids and names (continuation)
# version 3.1:  2025-02-12: Start an doi-author_name match exercise
# version 3.2:  2025-02-15: Continue with the doi-author_name match exercise(handle the duplicates)
# version 3.3:  2025-02-17: Continue with the doi-author_name match exercise (do the match for one author papers)
# version 3.4:  2025-02-18: Continue with the doi-author_name match exercise but generalize the loop and further handle OpenAlexId duplicates
# version 4.1:  2025-02-19: Send the functions to a source code file and perform the doi-author_name excercise iteratively within a function
# version 4.2:  2025-02-20: Get the authors openalex information to handle duplicated OpenAlex authors.
# version 5.1:  2025-02-21: Get the author basic information to do an OpenAlex duplicates excercise
# version 6.1:  2025-02-23: Handle the duplicates and concatenate the manually searched OpenAlex authors
# version 6.2:  2025-02-25: Create a global match between the AARC and OpenAlex authors
# From version 6.3 the source code is also called by the 02_aarc_twitter_match_via_openalex.py file
# version 6.4:  2025-03-06: Build an BusinessEcon Faculty List dictionary to be able to do the Twitter match
# version 6.5:  2025-03-10: Continue building the BusinessEcon Faculty dictionary
# version 6.6:  2025-03-27: Continue with the BusinessEcon Faculty dictionary, but translate the merge to a function
# version 7.1:  2025-03-31: See how many people from the DOI files were not matched by the first exercise
# version 8.1:  2025-04-02: Get the OpenAlex information for the papers (title,date keywords, fields).
# version 8.2:  2025-04-04: Continue with the OpenAlex information for the papers (title,date keywords, fields).
# version 8.3:  2025-04-05: Continue with the OpenAlex information for the papers (title,date keywords, fields).
# version 8.4:  2025-04-13: Handle the duplicates from the second excercise of names matching
# version 8.5:  2025-04-14: Continue with the duplicates from the second excercise of names matching, move to functions for efficiency
# version 9.1:  2025-04-15: Format the OpenAlex information for the papers (title,date keywords, fields).
# version 9.2:  2025-04-23: Continue with the format of the OpenAlex information for the papers (title,date keywords, fields).
# version 10.1: 2025-04-24: Get the openalex authors names and alternative names to check for potential change in names
# version 10.2: 2025-04-25: Continue with the openalex authors names and alternative names to check for potential change in names
# version 11.1: 2025-05-05: Produce a top-3 paper category per author
# version 11.2: 2025-05-12: Clean the top-3 paper category per author final file
# version 12.1: 2025-05-24: Build a per author topic scraper
# version 12.2: 2025-05-25: Produce an author topics classification file
# version 12.3: 2025-05-26: Continue with the author topics classification file (move the relevant code to a function)
# version 12.4: 2025-05-27: Send the previous author topics code to a function as legacy code
# version 12.5: 2025-05-29: Continue with the openalex authors names and alternative names to check for potential change in names
# version 12.6: 2025-06-01: Continue with the openalex authors names and alternative names to check for potential change in names

# Pseudo Code
# 00. Packages, Set Working directory and call the source code
# 01. Get the OpenAlex PaperId and AuthorId from a list of papers using the doi code
# 02. Doi-author_name match first exercise
# 03. Query the authors with duplicated PersonIds in aarc to get the number of works in their careers
# 04. Produce a first version of the dictionary for the reduced sample (the fn handles OpenAlexId duplicates and concatenates the manually searched OpenAlex authors)
# 05. Produce the 'final_data_usa_OpenAlexIds.csv' and 'final_data_nonusa_OpenAlexIds.csv' files
# 06. Do the match following Bernhard's matching procedure
# 07. Produce the 'BusinessEconFaculty' file with the OpenAlexIds of the faculty members (handle OpenAlexId duplicates and concatenate the manually searched OpenAlex authors)
# 08. Measure the progress of the match between the AARC and OpenAlex authors
# 09. Get other relevant information at the paper level (title, date, keywords, fields)
# 10. Get the alternative names of the authors that are in the BusinessEcon Faculty dictionary
# 11. Get the topics/subfields/fields/domains for the authors that are in the BusinessEcon Faculty dictionary

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
at_vp = aarc_oa.gen_authors_topics_via_papers(wd_path = wd_path) # Call the function to generate the authors topics classification DataFrame


# 10. Get the alternative names of the authors that are in the BusinessEcon Faculty dictionary
# 10.1 Generate the authors to call
authors_to_call = aarc_oa.gen_authors_to_call(wd_path) # Generate the authors to call
authors_batch   = aarc_oa.generate_id_batches(df = authors_to_call, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 21   # Set the number of batches to call the API (1 batch = 1000 authors)
# 10.2 Call the API to get the authors alternative names
for i in range(0,num_batches):          # Iterate over each batch
        print("Current batch: ", i)     # Print the current batch to the user
        authors_vec = authors_batch[i]  # Define the authors vector
        # Call the API to get the authors alternative names
        authors_alt_names_df = aarc_oa.linear_author_alternative_names(wd_path,authors_vec)
# 10.3 Save the final dataframe
n  = aarc_oa.gen_final_authors_alternative_names_csv(wd_path = wd_path)
an = aarc_oa.female_surname_change_excercise(wd_path = wd_path) # Call the function to run the

# 11. Get the topics/subfields/fields/domains for the authors that are in the BusinessEcon Faculty dictionary
# 11.1 Generate the authors to call
authors_to_call = aarc_oa.gen_author_topics_to_call(wd_path) # Generate the authors to call
authors_batch   = aarc_oa.generate_id_batches(df = authors_to_call, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 1   # Set the number of batches to call the API (1 batch = 1000 authors)
# 11.2 Call the API to get the authors alternative names
for i in range(0,num_batches):          # Iterate over each batch
        print("Current batch: ", i)     # Print the current batch to the user
        authors_vec = authors_batch[i]  # Define the authors vector
        # Call the API to get the authors alternative names
        authors_topics_df = aarc_oa.linear_author_topics_scraper(wd_path,authors_vec)
# 11.3 Save the final dataframe and produce the topics classification df
at, be_atc = aarc_oa.gen_final_author_topics_csv(wd_path = wd_path)




###################################################
# SEND THE PENDING TO BE MATCHED AUTHORS TO JORGE
# Step 1: Run the match_df code to get the match_df DataFrame (point #8. in this file).
# Step 2: Filter to get the people that are in the DOI files but have not been matched yet
df_pending = match_df[match_df['MatchStatus'] == 'DOI Only'] # Filter the DataFrame to only include rows where the 'PersonOpenAlexId' column is null
df_pending = df_pending[['PersonId','MatchStatus']] # Select the relevant columns
# Step 3: Merge the papers data with the df_pending 
fp01 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
df_doi_papers = pd.read_excel(fp01, sheet_name = "Sheet1") # Read the AARC people DOI file
df_doi_papers = df_doi_papers[['aarc_personid','doi','aarc_name']] # Select the relevant columns
df_doi_papers = df_doi_papers.rename(columns={'aarc_personid':'PersonId','doi':'DOI','aarc_name':'PersonName'}) # Rename the columns
df_doi_papers_pending = pd.merge(df_doi_papers, df_pending, on='PersonId', how='left') # Merge the two DataFrames on the 'PersonId' column
aarc_oa.check_duplicates_and_missing_values(original_df = df_doi_papers ,new_df = df_doi_papers_pending,column_name = 'MatchStatus', check_missing_values = False) # Check the duplicates and missing values
df_doi_papers_pending = df_doi_papers_pending[ df_doi_papers_pending['MatchStatus'] == 'DOI Only'] # Select the relevant columns
# Step 4: See how many of the papers with pending authors have actually been queried from OpenAlex
fp02 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
df_DOI_OA_papers = pd.read_csv(fp02) # Read the BusinessEcon Faculty list
df_DOI_OA_papers = df_DOI_OA_papers[['paper_doi','api_found']] # Select the AARCId and Names
df_DOI_OA_papers = df_DOI_OA_papers.rename(columns = {'paper_doi':'DOI'}) # Rename the columns
df_DOI_OA_papers = df_DOI_OA_papers.drop_duplicates() # Delete the duplicates
df_doi_papers_pending_oa = pd.merge(df_doi_papers_pending, df_DOI_OA_papers, on='DOI', how='left') # Merge the two DataFrames on the 'DOI' column
aarc_oa.check_duplicates_and_missing_values(original_df = df_doi_papers_pending ,new_df = df_doi_papers_pending_oa, column_name = 'api_found', check_missing_values = True) # Check the duplicates and missing values
# Step 5: Remove the authors that have papers with a failed OpenAlex query
# 5.1 Identify the authors
df_failed_oa_papers_authors = df_doi_papers_pending_oa.copy()
df_failed_oa_papers_authors = df_failed_oa_papers_authors[['PersonId','api_found']] # Select the relevant columns
df_failed_oa_papers_authors = df_failed_oa_papers_authors.drop_duplicates() # Delete the duplicates
df_failed_oa_papers_authors['PersonId_Count'] = df_failed_oa_papers_authors.groupby('PersonId')['PersonId'].transform('size')
df_failed_oa_papers_authors['Mixed_api_found'] = np.where(
        df_failed_oa_papers_authors['PersonId_Count'] == 1.0, 'No',
        np.where(
            (df_failed_oa_papers_authors['PersonId_Count'] > 1.0), 'Yes','Unknown'
        )
    )
df_failed_oa_papers_authors = df_failed_oa_papers_authors[df_failed_oa_papers_authors['Mixed_api_found'] == 'No']
df_failed_oa_papers_authors = df_failed_oa_papers_authors[df_failed_oa_papers_authors['api_found'] == 'No'] 
# 5.2 Save the failed authors for the record
df_failed_oa_papers_authors['Failed_Author'] = "Yes"
fp_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Jorge_matching_help\\failed_oa_papers_authors.xlsx"
df_failed_oa_papers_authors.to_excel(fp_path, index=False) # Save the file to check the results
# 5.3 Merge with the 'df_doi_papers_pending_oa' df and remove the failed authors
df_failed_oa_papers_authors = df_failed_oa_papers_authors[['PersonId','Failed_Author']] # Select the relevant columns
df_doi_papers_pending_oa_rfa = pd.merge(df_doi_papers_pending_oa, df_failed_oa_papers_authors, on='PersonId', how='left') # Merge the two DataFrames on the 'PersonId' column
aarc_oa.check_duplicates_and_missing_values(original_df = df_doi_papers_pending_oa ,new_df = df_doi_papers_pending_oa_rfa, column_name = 'Failed_Author', check_missing_values = False) # Check the duplicates and missing values
df_doi_papers_pending_oa_rfa = df_doi_papers_pending_oa_rfa[df_doi_papers_pending_oa_rfa['Failed_Author'] != 'Yes'] # Select the relevant columns
df_doi_papers_pending_oa_rfa = df_doi_papers_pending_oa_rfa.drop('Failed_Author', axis=1)
# Step 6: Save this file to be sent to Jorge
fp_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Jorge_matching_help\\aarc_doi_pending_matching.xlsx"
df_doi_papers_pending_oa_rfa.to_excel(fp_path, index=False) # Save the file to check the results
# Step 7. Produce the file that contains the OpenAlex data to be matched with
# 7.1 Get all the data queried from OpenAlex
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\\doi_papers_authors_openalex.csv"
df_doi_oa_papers = pd.read_csv(fp) # Read the BusinessEcon Faculty list
df_doi_oa_papers = df_doi_oa_papers.rename(columns = {'paper_doi':'DOI'}) # Rename the columns
# 7.2 Prepare the data
df_dois_to_merge = df_doi_papers_pending_oa_rfa[['DOI']] # Select the relevant columns
df_dois_to_merge = df_dois_to_merge.drop_duplicates() # Delete the duplicates
df_dois_to_merge['dummy_col'] = 1 # Create a dummy column to merge
# 7.3 Merge the two DataFrames to get the OpenAlex data
df_doi_oa_papers_match = pd.merge(df_doi_oa_papers, df_dois_to_merge, on='DOI', how='left') # Merge the two DataFrames on the 'DOI' column
aarc_oa.check_duplicates_and_missing_values(original_df = df_doi_oa_papers,new_df = df_doi_oa_papers_match, column_name = 'dummy_col', check_missing_values = False) # Check the duplicates and missing values
# 7.4 Keep the papers that are in the DOI files and in the OpenAlex data
df_doi_oa_papers_match = df_doi_oa_papers_match[df_doi_oa_papers_match['dummy_col'] == 1] # Select the relevant columns
# 7.5 Save the file to be sent to Jorge
fp_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Jorge_matching_help\\doi_papers_authors_openalex_reduced.xlsx"
df_doi_oa_papers_match.to_excel(fp_path, index=False) # Save the file to check the results

