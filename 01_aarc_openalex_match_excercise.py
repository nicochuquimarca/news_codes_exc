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

# Pseudo Code
# 0. Packages, Set Working directory and call the source code
# 1. Get the OpenAlex PaperId and AuthorId from a list of papers using the doi code 
# 2. Doi-author_name match first exercise
# 3. Query the authors with duplicated PersonIds in aarc to get the number of works in their careersS
# 4. Doi-author_name match second excercise (handle OpenAlexId duplicates and concatenate the manually searched OpenAlex authors)
# 5. Produce the 'final_data_usa_OpenAlexIds.csv' and 'final_data_nonusa_OpenAlexIds.csv' files
# 6. Produce a 'BusinessEconFaculty' file with the OpenAlexIds of the faculty members

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
authors_works_df  = aarc_oa.gen_authors_ids_to_call(wd_path = wd_path,dictionary_type = "aarc_openalex_author_intermediate_businessecon_dictionary" ) # Get the authors ids to call the API
authors_ids_batch = aarc_oa.generate_id_batches(df = authors_works_df, batch_size = 100) # Transform the DataFrame into batches
num_batches = 9    # Set the number of batches to call the API (1 batch = 100 authors)
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        authors_vec = authors_ids_batch[i] # Define the authors vector
        # Linear Process (used for debug and call the API in this case)
        authors_work = aarc_oa.linear_works_by_year(wd_path = wd_path,authors_vec = authors_vec)
# Save the final dataframe
x = aarc_oa.gen_final_works_by_year_csv(wd_path = wd_path)

# 4. Doi-author_name match second excercise (handle OpenAlexId duplicates and concatenate the manually searched OpenAlex authors)
author_dictionary  = aarc_oa.gen_aarc_openalex_dictionary(wd_path = wd_path, dictionary_type = "reduced sample")

# 5. Produce the 'final_data_usa_OpenAlexIds.csv' and 'final_data_nonusa_OpenAlexIds.csv' files
x = aarc_oa.gen_final_data_with_oa_ids(wd_path = wd_path, file_name = 'final_data_usa')
y = aarc_oa.gen_final_data_with_oa_ids(wd_path = wd_path, file_name = 'final_data_nonusa')

# 6. Produce a first 'BusinessEconFaculty' file with the OpenAlexIds of the faculty members (handle OpenAlexId duplicates and concatenate the manually searched OpenAlex authors)
author_businessecon_dictionary  = aarc_oa.gen_aarc_openalex_dictionary(wd_path = wd_path, dictionary_type = "full sample")


#### PENDING JOB: MAKE THIS A FUNCTION!
# 7. See the authors that are in the DOI files and in the BusinessEcon Faculty list but were not matched by the first exercise
# 7.1 Open the DOI files
f_path_01 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
df_DOI_01 = pd.read_excel(f_path_01, sheet_name = "Sheet1") # Read the AARC people DOI file
f_path_02 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\merged_AARC_DOI_Scopus_id_Yusuf.csv"
df_DOI_02 = pd.read_csv(f_path_02) # Read the AARC people DOI file
# 7.2 Select AARCId and Names and delete the duplicates
# 7.2.1 Work with DOI_01
df_01 = df_DOI_01[['aarc_personid','aarc_name']] # Select the AARCId and Names
df_01 = df_01.drop_duplicates() # Delete the duplicates
df_01 = df_01.rename(columns = {'aarc_personid':'PersonId','aarc_name':'PersonName'}) # Rename the columns
# 7.2.2. Work with DOI_02
df_02 = df_DOI_02[['personid','personname']] # Select the AARCId and Names
df_02 = df_02.drop_duplicates() # Delete the duplicates
df_02 = df_02.rename(columns = {'personid':'PersonId','personname':'PersonName'}) # Rename the columns
# 7.3 Concatenate the two dataframes and delete the duplicates
df_DOI = pd.concat([df_01,df_02],ignore_index=True) # Concatenate the two dataframes
df_DOI = df_DOI.drop_duplicates() # Delete the duplicates
# 7.4 Open the BusinessEcon Faculty dictionary
f_path_03 = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_businessecon_dictionary.xlsx"
be_dict_df = pd.read_excel(f_path_03, sheet_name = "Sheet1") # Read the BusinessEcon Faculty dictionary
be_dict_df = be_dict_df[['PersonId','PersonName']] # Select the AARCId and Names
be_dict_df = be_dict_df.drop_duplicates() # Delete the duplicates
# 7.5 Open the origina BusinessEcon Faculty list
f_path_04 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\BusinessEconFacultyLists.csv"
be_list_df = pd.read_csv(f_path_04) # Read the BusinessEcon Faculty list
be_list_df = be_list_df[['PersonId','PersonName']] # Select the AARCId and Names
be_list_df = be_list_df.drop_duplicates() # Delete the duplicates
# 7.6 Start the merges to see the authors that are in the BusinessEcon dictionary, in the DOI files and those who are in neither of them
#     The main file in which all the merges will take place is be_list_df
# 7.6.1 Merge with those in the BusinessEcon dictionary
be_dict_df_tm = be_dict_df[['PersonId']] # Select the AARCId
be_dict_df_tm['be_dict_dummy'] = 1       # Create a dummy variable to merge
be_list_df_01 = pd.merge(be_list_df,be_dict_df_tm,how='left',on='PersonId') # Merge the BusinessEcon Faculty list with the BusinessEcon dictionary
aarc_oa.check_duplicates_and_missing_values(original_df = be_list_df, new_df = be_list_df_01,column_name ='PersonId',check_missing_values = False) # Check the duplicates and missing values
# 7.6.2 Merge with those in the DOI files
df_DOI_tm = df_DOI[['PersonId']] # Select the AARCId
df_DOI_tm['DOI_dummy'] = 1       # Create a dummy variable to merge
df_DOI_tm = df_DOI_tm.drop_duplicates() # Delete the duplicates
be_list_df_02 = pd.merge(be_list_df_01,df_DOI_tm,how='left',on='PersonId') # Merge the BusinessEcon Faculty list with the DOI files
aarc_oa.check_duplicates_and_missing_values(original_df = be_list_df_01, new_df = be_list_df_02,column_name ='PersonId',check_missing_values = False) # Check the duplicates and missing values
# 7.7 Start to divide the data 
# 7.7.1 Create the 'MatchStatus' based on different conditions
be_list_df_02['MatchStatus'] = np.where(
    be_list_df_02['be_dict_dummy'] == 1.0, 'Matched',
    np.where(
        (be_list_df_02['DOI_dummy'] == 1.0) & (be_list_df_02['be_dict_dummy'].isnull()), 'DOI Only',
        np.where(
            be_list_df_02['DOI_dummy'].isnull() & be_list_df_02['be_dict_dummy'].isnull(), 'Not in DOI files', 'Unknown'
        )
    )
)
# Outcome by MatchStatus
# DOI Only	       = 11733 (41.42%)
# Matched	       = 12683 (44.77%)
# Not in DOI files = 3914  (13.82%)
# Conclusion I'll work with the 'DOI Only' group first
# Comment: When I checked the papers called from the API, there are some papers that were not retrieved.
work_df = be_list_df_02[be_list_df_02['MatchStatus'] == 'DOI Only'] # Select the 'DOI Only' group
# 7.8 See those who have at least one paper in the API and those who have no papers in the API
# 7.8.1 Get the all the DOIS
df_DOIr_01 = df_DOI_01[['aarc_personid','doi','aarc_name']] # Select the AARCId and Names
df_DOIr_01 = df_DOIr_01.rename(columns = {'aarc_personid':'PersonId','aarc_name':'PersonName'}) # Rename the columns
df_DOIr_02 = df_DOI_02[['personid','DOI','personname']] # Select the AARCId and Names
df_DOIr_02 = df_DOIr_02.rename(columns = {'personid':'PersonId','DOI':'doi','personname':'PersonName'}) # Rename the columns
df_DOIr = pd.concat([df_DOIr_01,df_DOIr_02],ignore_index=True) # Concatenate the two dataframes
df_DOIr = df_DOIr.drop_duplicates() # Delete the duplicates
df_DOIr['PersonId_str'] = df_DOIr['PersonId'] # Create a column to check for potential duplicates while doing the merge
df_DOIr['PersonId_str'] = df_DOIr['PersonId_str'].astype(str) # Change the type of the column to string
df_DOIr['dup_check'] = df_DOIr['PersonId_str'] + df_DOIr['doi'] # Create a key to merge the two datasets
df_DOIr = df_DOIr.drop(columns=['PersonId_str']) # Drop the column used to check for potential duplicates while doing the merge
df_DOIr = df_DOIr.drop_duplicates() # Delete the duplicates
# 7.8.2 Merge with the work_df to work only with those who have not been matched
work_df_tm = work_df[['PersonId','MatchStatus']] # Select the AARCId
work_df_tm = work_df_tm.drop_duplicates() # Delete the duplicates
df_DOIr_m = pd.merge(df_DOIr,work_df_tm,how='left',on='PersonId') # Merge the BusinessEcon Faculty list with the DOI files
aarc_oa.check_duplicates_and_missing_values(original_df = df_DOIr, new_df = df_DOIr_m,column_name ='dup_check',check_missing_values = False) # Check the duplicates and missing values
# 7.8.3 Filter those who were made match
df_DOIr_m = df_DOIr_m[df_DOIr_m['MatchStatus'] == 'DOI Only'] # Select the 'DOI Only' group
# 7.8.4 Open the 'doi_papers_authors_openalex' file
f_path_05 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
df_DOI_papers = pd.read_csv(f_path_05) # Read the BusinessEcon Faculty list
df_DOI_papers = df_DOI_papers[['paper_doi','api_found']] # Select the AARCId and Names
df_DOI_papers = df_DOI_papers.rename(columns = {'paper_doi':'doi'}) # Rename the columns
df_DOI_papers = df_DOI_papers.drop_duplicates() # Delete the duplicates
# 7.8.5 Merge df_DOIr_m with df_DOI_papers to see those who have at least one paper in the API and those who have no papers in the API
df_DOIr_mm = pd.merge(df_DOIr_m,df_DOI_papers,how='left',on='doi') # Merge the BusinessEcon Faculty list with the DOI files
# 7.8.6 Get a diagnosis of those authors who have at least one paper in the API and those who have no papers found by the API
#       Similar to the group by done in R. Continue after dinner.
df_DOIr_mm['api_found_num'] = np.where(
    df_DOIr_mm['api_found'] == 'Yes', 1.0,
    np.where(
        df_DOIr_mm['api_found'] == 'Yes', 0.0, 0.0
        )
    )
df_DOIr_mmg = df_DOIr_mm[['PersonId','PersonName','api_found_num']] # Select the AARCId and Names
df_DOIr_mmg = df_DOIr_mmg.groupby(['PersonId', 'PersonName'], as_index=False)['api_found_num'].sum()
df_DOIr_mmg_af  = df_DOIr_mmg[df_DOIr_mmg['api_found_num'] > 0] # Select the 'DOI Only' group
df_DOIr_mmg_anf = df_DOIr_mmg[df_DOIr_mmg['api_found_num'] == 0] # Select the 'DOI Only' group
df_DOIr_mmg_af.shape[0]
df_DOIr_mmg_anf.shape[0]
# Conclusion: We have the information for almost all of the 11713 authors in the DOI files. I need to work on improving the match. 
#             Not calling the API

# 8. Get Bernhard's help with the match. Prepare the two databases to send him and ask him to send me the match
DOIr_mm_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_doi_pending_matching.csv"
df_DOIr_mm.to_csv(DOIr_mm_path, index=False) # Save the file to send to Bernhard
# The other file shared with Berhnard is the 'doi_papers_authors_openalex.csv' file 


# 9. Get other relevant information at the paper level (title, date, keywords, fields)
#Linear Scraper performance: 445 seconds
#Parallel Scraper performance: 120 seconds, The Parallel scraper is 2.71 times faster.
    # 9.1 Generate the papers to call
papers_to_call   = aarc_oa.gen_papers_doi_to_call(wd_path,source='aarc',scrap_fn = 'paper_info') # Generate the papers to call
papers_doi_batch = aarc_oa.generate_id_batches(df = papers_to_call, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 100   # Set the number of batches to call the API (1 batch = 1000 papers)
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