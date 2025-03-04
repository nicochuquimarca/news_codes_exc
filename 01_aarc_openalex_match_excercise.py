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
papers_to_call   = aarc_oa.gen_papers_doi_to_call(wd_path,source='aarc')         # Generate the papers to call
papers_doi_batch = aarc_oa.generate_id_batches(df = papers_to_call, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 8  # Set the number of batches to call the API (1 batch = 1000 papers)
    # 1.2 Call the API to get the authors openalex ids and names
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        doi_vec = papers_doi_batch[i] # Define the authors vector
        # Linear Process (used for debug)
        # paper_authors_df = aarc_oa.linear_paper_authors(wd_path,doi_vec)
        # Parallel Process (used to call the API efficiently)
        paper_authors_df = aarc_oa.parallel_paper_authors(wd_path,doi_vec)
    # 1.2 Save the final dataframe
z = aarc_oa.gen_final_papers_authors_csv(wd_path = wd_path) # This function generates the 'doi_papers_authors_openalex.csv' file

# 2. Doi-author_name match first exercise
author_inter_dict_df = aarc_oa.doi_author_surname_match_excercise(wd_path = wd_path)

# 3. Query the authors with duplicated PersonIds in aarc to get the number of works in their careersS
authors_works_df  = aarc_oa.gen_authors_ids_to_call(wd_path = wd_path, ) # Get the authors ids to call the API
authors_ids_batch = aarc_oa.generate_id_batches(df = authors_works_df, batch_size = 100) # Transform the DataFrame into batches
num_batches = 6    # Set the number of batches to call the API (1 batch = 100 authors)
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        authors_vec = authors_ids_batch[i] # Define the authors vector
        # Linear Process (used for debug and call the API in this case)
        authors_work = aarc_oa.linear_works_by_year(wd_path = wd_path,authors_vec = authors_vec)
# Save the final dataframe
x = aarc_oa.gen_final_works_by_year_csv(wd_path = wd_path)

# 4. Doi-author_name match second excercise (handle OpenAlexId duplicates and concatenate the manually searched OpenAlex authors)
# 4.1 Open the intermediate dictionary and isolate the duplicates and the missing values
fp = wd_path +  "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_dictionary.xlsx"
dict_df = pd.read_excel(fp)
missing_authors_df  = dict_df[dict_df['matched'] == 0] # Missing authors df
nmissing_authors_df = dict_df[dict_df['matched'] == 1] # Matched authors df
# 4.2 Solve the OpenAlexId duplicates problem
# 4.2.1 Divide the df between unique and duplicates authors
nmissing_authors_df['PersonId_dups'] = nmissing_authors_df['PersonId'].map(nmissing_authors_df['PersonId'].value_counts()) # Count the duplicates
unique_authors_df = nmissing_authors_df[nmissing_authors_df['PersonId_dups'] == 1] # Get the unique authors
duplicates_authors_df = nmissing_authors_df[nmissing_authors_df['PersonId_dups'] > 1] # Get the duplicates authors
# 4.2.2. Use an auxiliary file to know which authors to keep
aux_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_handle_duplicates_manual_file.xlsx"
aux_df = pd.read_excel(aux_fp,sheet_name="Sheet1") # Open the auxiliary file
sel_cols = ['PersonOpenAlexId','keep']
aux_df = aux_df[sel_cols] # Select the columns
duplicates_authors_df = pd.merge(duplicates_authors_df, aux_df, on = 'PersonOpenAlexId', how = 'left') # Merge the two datasets
duplicates_authors_df = duplicates_authors_df[duplicates_authors_df['keep'] == 'Yes'] # Keep only the authors to keep
# 4.2.3 Do a test and then concatenate the unique dataset with the corrected duplicates authors df and do a final check
duplicates_authors_df['PersonId_dups_new'] = duplicates_authors_df['PersonId'].map(duplicates_authors_df['PersonId'].value_counts()) # Count the duplicates
test_unique_values = duplicates_authors_df['PersonId_dups_new'].unique() # Get the unique values
sel_cols = ['PersonId','PersonOpenAlexId','PersonId_dups_new']
duplicates_authors_df = duplicates_authors_df.drop(columns=['keep','PersonId_dups_new']) # Drop non useful columns
nmissing_authors_new_df = pd.concat([unique_authors_df, duplicates_authors_df], ignore_index=True)
nmissing_authors_new_df = nmissing_authors_new_df.drop(columns=['PersonId_dups']) # Drop non useful columns
# 4.3 Concatenate the missing authors with the new authors
man_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\manual_aarc_openaalex_matches.xlsx"
manual_authors_df = pd.read_excel(man_fp)
manual_authors_df = manual_authors_df.drop(columns=['Comment','Resources']) # Drop non useful columns
# 4.3.1 Do a final check to know that all the missing authors are in the manual authors df
manual_aux_df = manual_authors_df[['PersonId']]
manual_aux_df['dummy'] = 1  
missing_authors_df = pd.merge(missing_authors_df, manual_aux_df, on = 'PersonId', how = 'left') # Merge the two datasets
test_df = missing_authors_df[missing_authors_df['dummy'] != 1] # Get the missing authors
test_df.shape[0] # All of the missing authors are in the manual authors df! :)
# 4.3.2 Concatenate the missing authors with the new authors
manual_authors_df['matched'] = False # Add a new column to the manual authors df to know that they were not originally matched
final_authors_df = pd.concat([manual_authors_df, nmissing_authors_new_df], ignore_index=True)
# 4.4 Save the final authors dictionary
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_dictionary.xlsx"
#final_authors_df.to_excel(file_path, index = False)

# 5. Produce the 'final_data_usa_OpenAlexIds.csv' and 'final_data_nonusa_OpenAlexIds.csv' files
x = aarc_oa.gen_final_data_with_oa_ids(wd_path = wd_path, file_name = 'final_data_usa')
y = aarc_oa.gen_final_data_with_oa_ids(wd_path = wd_path, file_name = 'final_data_nonusa')


#### EXPERIMENTAL CODE ####

### MODIFY THIS FUNCTION LATER TO CREATE A COMPREHENSIVE DICTIONARY ON THESE AUTHORS. ####### 


# Fn31: gen_final_data_with_oa_ids = Generate the final data with the OpenAlex ids
def gen_final_data_with_oa_ids(wd_path,file_name):
    # 5.1 Open the data files
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\"+file_name+".csv"
    df = pd.read_csv(fp)    
    df['PersonId'] = df['PersonId'].astype(str)
    # 5.2 Open the final authors dictionary
    dict_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_dictionary.xlsx"
    dict_df = pd.read_excel(dict_fp)
    dict_df['PersonId'] = dict_df['PersonId'].astype(str)
    dict_df = dict_df.drop(columns=['matched']) # Drop the matched column
    # 5.3 Merge the data with the dictionary and do a check
    oa_authorid_df = pd.merge(df, dict_df, on = "PersonId", how = "left") # Merge with the dictionary
    aarc_oa.check_duplicates_and_missing_values(original_df = df, new_df = oa_authorid_df, column_name='PersonOpenAlexId', check_missing_values = False)
    return oa_authorid_df
    # 5.4 Reorder columns for export purposes
    oa_authorid_df = aarc_oa.reorder_columns(df = oa_authorid_df, column_name = 'PersonOpenAlexId', position = 1)
    oa_authorid_df = aarc_oa.reorder_columns(df = oa_authorid_df, column_name = 'PersonName', position = 8)
    oa_authorid_df = aarc_oa.reorder_columns(df = oa_authorid_df, column_name = 'PersonOpenAlexName', position = 9)
    # 5.6 Merge the data from the Institutions
    inst_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Final_sample_AARC_IPEDS_filtered_with_id.csv"
    inst_df = pd.read_csv(inst_fp)
    inst1_df = aarc_oa.format_dictionary(inst_df,'Institution')
    oa_authorid_format_df = aarc_oa.format_faculty(oa_authorid_df,'InstitutionId') # Format the faculty
    oa_authorid_inst_df = pd.merge(oa_authorid_format_df, inst1_df, on = "InstitutionId", how = "left") # Merge with the institution
    aarc_oa.check_duplicates_and_missing_values(original_df = oa_authorid_format_df, new_df = oa_authorid_inst_df, column_name='InstitutionOpenAlexId')
    # 5.7 Reorder columns for export purposes
    oa_authorid_inst_df = aarc_oa.reorder_columns(df = oa_authorid_inst_df, column_name = 'InstitutionOpenAlexId', position = 3)
    oa_authorid_inst_df = aarc_oa.reorder_columns(df = oa_authorid_inst_df, column_name = 'InstitutionOpenAlexName', position = 5)
    # 5.8 Save the final data
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\"+ file_name + "_OpenAlexIds.csv"
    oa_authorid_inst_df.to_csv(file_path, index = False)
    print("The file was saved in: ", file_path)
    return oa_authorid_inst_df
        


# 6. Produce a 'BusinessEconFaculty' file with the OpenAlexIds of the faculty members
y = gen_final_data_with_oa_ids(wd_path = wd_path, file_name = 'BusinessEconFacultyLists')


y


fp = wd_path +  "\\data\\raw\\aarc_openalex_match\\input_files\\BusinessEconFacultyLists.csv"
faculty_df = pd.read_csv(fp)

faculty_df.columns
faculty_df.dtypes
