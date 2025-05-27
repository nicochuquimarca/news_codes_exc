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
n = aarc_oa.gen_final_authors_alternative_names_csv(wd_path = wd_path)

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
# 11.3 Save the final dataframe
at = aarc_oa.gen_final_author_topics_csv(wd_path = wd_path)


##################################################################
# MOQI AGGREGATION EXCERCISE ###
# This will become a function in the future (in point 9.)
# 1. Concatenate all the files 
def concat_fn(wd_path):
    # 1. Select the folder path where the files are located
    folder_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_info"
    files_vector = os.listdir(folder_path) # Get all the files in the folder
    # 2. Initialize an empty list to store DataFrames
    dfs = []
    # 3. Iterate over each file in the directory
    for file in files_vector:
       if file.endswith('.csv'):
          file_path = os.path.join(folder_path, file) # Get the file path
          df = pd.read_csv(file_path)
          dfs.append(df)
    # 4. Concatenate all DataFrames in the list into a single DataFrame
    final_df = pd.concat(dfs, ignore_index=True)
    df = final_df.copy() # Create a copy of the final DataFrame
    # 5. Drop duplicates
    df = df.drop_duplicates() # Drop duplicates
    # 6. Diagnose the number of topics per paper, and print the unique values to the user
    df['topics_count'] = df['paper_topics_vec'].str.count('}')/4
    t_count = df['topics_count'].unique()
    print(f"The unique values for number of topics per paper are {t_count}" )
    # 7. Separat the dataframes for those with Nan, 0, 1 or more than 1 topics
    df_ntopics = df[df['topics_count'].isna()] # Select the papers with no topics
    df_0topics = df[df['topics_count'] == 0  ] # Select the papers with 0 topics
    df_mtopics = df[df['topics_count'] >= 1  ] # Select the papers with 1 or more topics
    # 8. Get the topics data for each type of dataframe
    df_ntopics = aarc_oa.extract_topics(df = df_ntopics,null_or_zero_topics = True)  # Get the topics data for the papers with no topics
    df_0topics = aarc_oa.extract_topics(df = df_0topics,null_or_zero_topics = True)  # Get the topics data for the papers with 0 topics
    df_mtopics = aarc_oa.extract_topics(df = df_mtopics,null_or_zero_topics = False) # Get the topics data for the papers with 1 or more topics
    fdf = pd.concat([df_mtopics,df_0topics,df_ntopics], axis = 0, ignore_index = True)  # Concatenate the dataframes
    return fdf # Return the final dataframe
    
x = concat_fn(wd_path) # Call the function to concatenate the files 

df = x.copy() # Create a copy of the final DataFrame

# Group by author_id and the topic-subfield combination columns, then count occurrences
agg_df = df.groupby(['author_id', 'topic_id_1', 'topic_name_1', 'topic_subfield_id_1', 'topic_subfield_name_1']).size().reset_index(name='Counts')

# Sort the DataFrame by 'author_id' and 'Counts' in descending order
agg_df = agg_df.sort_values(by=['author_id', 'Counts'], ascending=[True, False])
# Create a ranking variable for 'Counts' in descending order by each 'author_id'
agg_df['CountsRanking'] = agg_df.groupby('author_id')['Counts'].rank(method='first', ascending=False).astype(int)
# Filter only the top 3 topics for each author
agg_df = agg_df[agg_df['CountsRanking'] <= 3]
# Create a key for the future merge with the classification
agg_df['key'] = agg_df['topic_id_1'] + agg_df['topic_subfield_id_1']
# Remove the AuthorsId that are 'A9999999999' or the Null values
agg_df = agg_df[agg_df['author_id'] != 'A9999999999'] # Filter the DataFrame to only include rows where the 'author_id' column is not null
agg_df = agg_df[agg_df['author_id'].notnull()] # Filter the DataFrame to only include rows where the 'author_id' column is not null
agg_df = agg_df[agg_df['author_id'] != '[None]'] # Filter the DataFrame to only include rows where the 'author_id' column is not null
# Pivot the DataFrame to wide format
wide_df = agg_df.pivot(index='author_id', columns='CountsRanking', values=['topic_id_1','topic_name_1','topic_subfield_id_1','topic_subfield_name_1','Counts','key'])
# Flatten the multi-level columns for better readability
wide_df.columns = [f"{col[0]}_Rank{col[1]}" for col in wide_df.columns]
# Reset the index to make 'author_id' a column
wide_df = wide_df.reset_index()
# Change name for the classification merges
wide_df = wide_df.rename(columns={'key_Rank1': 'key1', 'key_Rank2': 'key2', 'key_Rank3': 'key3'})

test = wide_df[wide_df['key1'].isna()] # Check if there are any authors with no topics
test.shape[0]

# CHANGE THIS WHEN GIVEN THE FINAL CLASSIFICATION
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\topics_dictionary_openalex_MG20250512.xlsx"
class_df = pd.read_excel(fp) # Read the Excel file into a DataFrame
class_df = class_df[class_df['Keep'] == "Yes"] # Filter to delete some duplicates that were introduced due to horrible prior programming from Nico :(
sel_cols = ['topic_subfield_key','Research_topic','Research_category' ]
class_df = class_df[sel_cols] # Select the columns to keep
class_df = class_df.rename(columns={'topic_subfield_key': 'key1','Research_topic' : 'Research_topic1', 'Research_category': 'Research_category1' })
class_df = class_df.drop_duplicates() # Drop duplicates

# DO THE MERGES
wide_df1 = pd.merge(wide_df, class_df, on='key1', how='left') # Merge the DataFrames on the 'key1' column
aarc_oa.check_duplicates_and_missing_values(wide_df,wide_df1,'Research_topic',check_missing_values = False)
class_df = class_df.rename(columns={'key1': 'key2', 'Research_topic1' : 'Research_topic2', 'Research_category1': 'Research_category2'}) # Rename the columns to avoid duplicates
wide_df2 = pd.merge(wide_df1, class_df, on='key2', how='left') # Merge the DataFrames on the 'key1' column
aarc_oa.check_duplicates_and_missing_values(wide_df1,wide_df2,'Research_topic',check_missing_values = False)
class_df = class_df.rename(columns={'key2': 'key3','Research_topic2' : 'Research_topic3', 'Research_category2': 'Research_category3'})
wide_df3 = pd.merge(wide_df2, class_df, on='key3', how='left') # Merge the DataFrames on the 'key1' column
aarc_oa.check_duplicates_and_missing_values(wide_df2,wide_df3,'Research_topic',check_missing_values = False)
sel_cols = ['author_id',
            'Research_category1','Research_topic1','Counts_Rank1','key1',
            'Research_category2','Research_topic2','Counts_Rank2','key2',
            'Research_category3','Research_topic3','Counts_Rank3','key3']
wide_fdf = wide_df3[sel_cols] # Select the columns to keep

aa_df = wide_df3[wide_df3['author_id'] == "A5083494527"] # Alberto Alesina
ed_df = wide_df3[wide_df3['author_id'] == "A5015565298"] # Esther Dufflo

exp_df = pd.concat([aa_df,ed_df], axis = 0, ignore_index = True) # Concatenate the dataframes
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\exp_df.xlsx"
exp_df.to_excel(fp, index=False) # Save the DataFrame to an Excel file


# OPEN THE BUSINESS ECON DICTIONARY AND STAY ONLY WITH AUTHORS THAT APPEAR THERE
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_businessecon_dictionary.xlsx"
be_df = pd.read_excel(fp) # Read the Excel file into a DataFrame
be_df = be_df.rename(columns={'PersonOpenAlexId': 'author_id'}) # Rename the column to match the other DataFrame
be_df = be_df[be_df['author_id'] != 'A9999999999'] # Filter the DataFrame to only include rows where the 'PersonOpenAlexId' column is not null


# DO THE MERGE WITH THE wide_fdf
be_topics_df = pd.merge(be_df, wide_fdf, on='author_id', how='left') # Merge the DataFrames on the 'key1' column
aarc_oa.check_duplicates_and_missing_values(be_topics_df,be_df,'Research_topic',check_missing_values = False)

# Sort the DataFrame by the 'AutoMatch' variable in descending order
be_topics_df = be_topics_df.sort_values(by='AutoMatch', ascending=False)

file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_authors_topics_classification.xlsx"
be_topics_df.to_excel(file_path, index=False) # Save the DataFrame to an Excel file



#####################################################################
# MOQI AGGREGATION EXCERCISE 2 --> WORK WITH AT THE AUTHOR LEVEL ####
# This should be added at point 11.
# Point 0. Open the author_topics file
at = aarc_oa.gen_final_author_topics_csv(wd_path = wd_path)
at_df = at.copy()               # Create a copy of the final DataFrame
at_df = at_df.drop_duplicates() # Drop duplicates
# 1. Do formatting to the author_topics file
# 1.1 Stay only with the three most relevant topics per author
at_df = at_df[at_df['topic_counter']<=3]                
# 1.2 Generate the topic_id and subfield_id keys
at_df['subfield_id'] = at_df['subfield_id'].fillna(0)
at_df['subfield_id'] = at_df['subfield_id'].astype(int) # Convert from float to int
at_df['subfield_id'] = at_df['subfield_id'].astype(str) # Convert the 'subfield_id' column to string type
at_df['topic_subfield_key'] = at_df['topic_id'] + at_df['subfield_id'] # Create a new column 'topic_subfield_key' that concatenates the 'topic_id' and 'subfield_id' columns
# 2. Open the topics classification file and do some formatting
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\topics_dictionary_openalex_MG20250512.xlsx"
class_df = pd.read_excel(fp)                   # Read the Excel file into a DataFrame
class_df = class_df[class_df['Keep'] == "Yes"] # Filter to delete some duplicates that were introduced due to horrible prior programming from Nico :(
sel_cols = ['topic_subfield_key','Research_topic','Research_category' ]
class_df = class_df[sel_cols] # Select the columns to keep
# 3. Do the merge between the author_topics and the topics classification
atc_df = pd.merge(at_df, class_df, on='topic_subfield_key', how='left') # Merge the DataFrames on the 'topic_subfield_key' column
aarc_oa.check_duplicates_and_missing_values(at_df,atc_df,'Research_topic',False)
# 4. Transform the DataFrame to wide format
wide_atc_df = atc_df.pivot(index=['id', 'display_name'], columns='topic_counter')     # Pivot the DataFrame to wide format
wide_atc_df.columns = ['{}_{}'.format(col[0], col[1]) for col in wide_atc_df.columns] # Flatten the multi-level columns for better readability
wide_atc_df = wide_atc_df.reset_index()
# 5. Do some formatting to the wide DataFrame
sel_cols = ['id', 'display_name',
            'Research_category_1','Research_topic_1','topic_display_name_1','subfield_display_name_1', 'topic_count_1',  
            'Research_category_2','Research_topic_2','topic_display_name_2','subfield_display_name_2', 'topic_count_2',
            'Research_category_3','Research_topic_3','topic_display_name_3','subfield_display_name_3', 'topic_count_3',
            'topic_subfield_key_1', 'topic_subfield_key_2', 'topic_subfield_key_3'
            ]
atc_fdf = wide_atc_df[sel_cols] # Select the columns to keep
atc_fdf = atc_fdf.rename(columns={'id': 'PersonOpenAlexId','display_name': 'PersonOpenAlexName'}) # Rename the column to match the other DataFrame
# 6. Merge with the AARC OpenAlex dictionary to get the PersonId and PersonName
# 6.1 Open the AARC OpenAlex dictionary to get the PersonId and PersonName and do some formatting
fp = wd_path +"\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_businessecon_dictionary.xlsx"
be_df = pd.read_excel(fp) # Read the Excel file into a DataFrame
sel_cols = ['PersonId','PersonName','PersonOpenAlexId','AutoMatch'] # Select the columns to keep
be_df = be_df[sel_cols] # Select the columns to keep
be_df = be_df.drop_duplicates() # Drop duplicates
# 6.2 Do the merge between the atc_fdf and the be_df
be_atc_df = pd.merge(be_df, atc_fdf, on='PersonOpenAlexId', how='left') # Merge the DataFrames on the 'PersonOpenAlexId' column
aarc_oa.check_duplicates_and_missing_values(be_atc_df,be_df,'Research_category_1',False) # Check for duplicates and missing values
be_atc_df = be_atc_df.sort_values(by='AutoMatch', ascending=False) # Sort the DataFrame by the 'share' column in descending order
# 7. Save the final DataFrame
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_authors_topics_classification_new.xlsx"
be_atc_df.to_excel(fp, index=False) # Save the DataFrame to an Excel file




#######################################################
# CHANGE OF SURNAME EXCERCISE ###

# Functions
def gen_surname_alternative_and_run_test(df, var_name,num_alternatives):
    # Get only the surnames from the regular name and the alternative name
    df[f'SurnameAlternative{num_alternatives}'] = df.apply(
            lambda row: row[f'{var_name}_{row[f"{var_name}_tot"]}'], axis=1) # Get the surname from the OpenAlex data
    df[f'SurnameAlternative{num_alternatives}'] = df[f'SurnameAlternative{num_alternatives}'].apply(unidecode) # Remove accents
    df[f'SurnameAlternative{num_alternatives}'] = df[f'SurnameAlternative{num_alternatives}'].str.replace('-', '') # Remove hyphens
    # Compare if the surnames are the same
    df[f'SurnameComparison{num_alternatives}'] = np.where(
        df[f'SurnameAlternative{num_alternatives}'] == df['SurnameBenchmark'], 0.0, 1.0)
    # Return the modified DataFrame
    return df




# Preamble: Generate the authors alternative names DataFrame
# 0. Generate the df
n = aarc_oa.gen_final_authors_alternative_names_csv(wd_path = wd_path)

# 1. Filter women authors
# 1.1 Open the BusinessEcon Faculty dictionary to get the AARC PersonId
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_businessecon_dictionary.xlsx"
aarc_oa_df = pd.read_excel(file_path, sheet_name = 'Sheet1')
aarc_oa_df = aarc_oa_df[['PersonId','PersonName','PersonOpenAlexId']]
aarc_oa_df = aarc_oa_df[ aarc_oa_df['PersonOpenAlexId'] != 'A9999999999'] # Filter the DataFrame to only include rows where the 'PersonOpenAlexId' column is not null
aarc_oa_df = aarc_oa_df.rename(columns={'PersonOpenAlexId': 'id'}) # Rename the column to match the other DataFrame
# 1.2 Open the original BusinessEcon Faculty list and get the gender information
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\BusinessEconFacultyLists.csv"
be_fac_df = pd.read_csv(file_path, encoding='latin1') # Read the CSV file into a DataFrame   
be_fac_df = be_fac_df[['PersonId','Gender']]
be_fac_df = be_fac_df.drop_duplicates()
# 1.3 Merge the two DataFrames to get the Gender information
aarc_oa_be_fac_df = pd.merge(aarc_oa_df, be_fac_df, on='PersonId', how='left') # Merge the two DataFrames on the 'PersonId' column
aarc_oa.check_duplicates_and_missing_values(original_df = aarc_oa_df ,new_df = aarc_oa_be_fac_df,column_name = 'Gender', check_missing_values = False)
# 1.4 Merge with the authors alternative names DataFrame
n_df = pd.merge(n, aarc_oa_be_fac_df, on='id', how='left') # Merge the two DataFrames on the 'id' column
aarc_oa.check_duplicates_and_missing_values(original_df = n ,new_df = n_df, column_name = 'PersonId', check_missing_values = False)
# 1.5 Get to know how many alternative names are there and their frecuency
summ_df = n.groupby('alternative_names_num').size().reset_index(name='Counts')
total_c  = summ_df['Counts'].sum() # Calculate the total of the 'Counts' column
summ_df['share'] = summ_df['Counts'] / total_c # Add a new column 'share' that shows the share of each count with respect to the total
summ_df = summ_df.sort_values(by='share', ascending=False) # Sort the DataFrame by the 'share' column in descending order
print("Decision: The test excercise will be carried on, until the alternative name 10.")
# 1.6 Unpack the alternative names in different columns
# Parse the JSON-like strings into Python lists
n_df['display_name_alternatives'] = n_df['display_name_alternatives'].apply(ast.literal_eval)
# Dynamically create new columns for alternative names
max_alternatives = n_df['alternative_names_num'].max()  # Find the maximum number of alternatives
for i in range(1, max_alternatives + 1):
    n_df[f'name{i:02d}'] = n_df['display_name_alternatives'].apply(lambda x: x[i - 1] if i <= len(x) else '')
# Drop all columns in n_df that match the pattern 'name11' to 'name63'
columns_to_drop = [col for col in n_df.columns if col.startswith('name') and 11 <= int(col[4:]) <= 63]
n_df = n_df.drop(columns=columns_to_drop)


# 1.7 Create the SurnameBenchmark in which we will compare the surnames
n_df = aarc_oa.format_authors_names(df = n_df, var_name ='display_name', message=True) # Split the regular name in pieces
# Get only the surnames from the regular name and the alternative name
n_df['SurnameBenchmark'] = n_df.apply(
        lambda row: row[f'display_name_{row["display_name_tot"]}'], axis=1) # Get the surname from the OpenAlex data
n_df['SurnameBenchmark'] = n_df['SurnameBenchmark'].apply(unidecode) # Remove accents
n_df['SurnameBenchmark'] = n_df['SurnameBenchmark'].str.replace('-', '') # Remove hyphens
# Drop all columns in n_df that match the pattern 'display_name_1' to 'display_name_11'
columns_to_drop = ['display_name_1','display_name_2','display_name_3','display_name_4',
                   'display_name_5','display_name_6','display_name_7','display_name_8', 
                   'display_name_9','display_name_10','display_name_11',
                   'display_name_tot']
n_df = n_df.drop(columns=columns_to_drop)


# FnXX: gender_surname_comparison = Check if any of the alternative names is different as the regular name
def gender_surname_comparison(df):
    # Step 1. Divide the df into multiple dfs based on the number of alternative names
    n_df01 = df[df['alternative_names_num'] == 1]  # Get the dataframe with 1 alternative name
    n_df02 = df[df['alternative_names_num'] == 2]  # Get the dataframe with 2 alternative names
    n_df03 = df[df['alternative_names_num'] == 3]  # Get the dataframe with 3 alternative names
    n_df04 = df[df['alternative_names_num'] == 4]  # Get the dataframe with 4 alternative names
    n_df05 = df[df['alternative_names_num'] == 5]  # Get the dataframe with 5 alternative names
    n_df06 = df[df['alternative_names_num'] == 6]  # Get the dataframe with 6 alternative names
    n_df07 = df[df['alternative_names_num'] == 7]  # Get the dataframe with 7 alternative names
    n_df08 = df[df['alternative_names_num'] == 8]  # Get the dataframe with 8 alternative names
    n_df09 = df[df['alternative_names_num'] == 9]  # Get the dataframe with 9 alternative names
    n_df10 = df[df['alternative_names_num'] == 10] # Get the dataframe with 10 alternative names
    # Step 2. Divide the names into different columns
    # Names = 1
    n_df01 = aarc_oa.format_authors_names(df = n_df01, var_name ='name01', message=True)  # Split the alternative name in pieces
    # Names = 2
    n_df02 = aarc_oa.format_authors_names(df = n_df02, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df02 = aarc_oa.format_authors_names(df = n_df02, var_name ='name02', message=True)    # Split the alternative name in pieces
    # Names = 3
    n_df03 = aarc_oa.format_authors_names(df = n_df03, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df03 = aarc_oa.format_authors_names(df = n_df03, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df03 = aarc_oa.format_authors_names(df = n_df03, var_name ='name03', message=True)    # Split the alternative name in pieces
    # Names = 4
    n_df04 = aarc_oa.format_authors_names(df = n_df04, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df04 = aarc_oa.format_authors_names(df = n_df04, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df04 = aarc_oa.format_authors_names(df = n_df04, var_name ='name03', message=True)    # Split the alternative name in pieces
    n_df04 = aarc_oa.format_authors_names(df = n_df04, var_name ='name04', message=True)    # Split the alternative name in pieces
    # Names = 5
    n_df05 = aarc_oa.format_authors_names(df = n_df05, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df05 = aarc_oa.format_authors_names(df = n_df05, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df05 = aarc_oa.format_authors_names(df = n_df05, var_name ='name03', message=True)    # Split the alternative name in pieces
    n_df05 = aarc_oa.format_authors_names(df = n_df05, var_name ='name04', message=True)    # Split the alternative name in pieces
    n_df05 = aarc_oa.format_authors_names(df = n_df05, var_name ='name05', message=True)    # Split the alternative name in pieces
    # Names = 6
    n_df06 = aarc_oa.format_authors_names(df = n_df06, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df06 = aarc_oa.format_authors_names(df = n_df06, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df06 = aarc_oa.format_authors_names(df = n_df06, var_name ='name03', message=True)    # Split the alternative name in pieces
    n_df06 = aarc_oa.format_authors_names(df = n_df06, var_name ='name04', message=True)    # Split the alternative name in pieces
    n_df06 = aarc_oa.format_authors_names(df = n_df06, var_name ='name05', message=True)    # Split the alternative name in pieces
    n_df06 = aarc_oa.format_authors_names(df = n_df06, var_name ='name06', message=True)    # Split the alternative name in pieces
    # Names = 7
    n_df07 = aarc_oa.format_authors_names(df = n_df07, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df07 = aarc_oa.format_authors_names(df = n_df07, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df07 = aarc_oa.format_authors_names(df = n_df07, var_name ='name03', message=True)    # Split the alternative name in pieces
    n_df07 = aarc_oa.format_authors_names(df = n_df07, var_name ='name04', message=True)    # Split the alternative name in pieces
    n_df07 = aarc_oa.format_authors_names(df = n_df07, var_name ='name05', message=True)    # Split the alternative name in pieces
    n_df07 = aarc_oa.format_authors_names(df = n_df07, var_name ='name06', message=True)    # Split the alternative name in pieces
    n_df07 = aarc_oa.format_authors_names(df = n_df07, var_name ='name07', message=True)    # Split the alternative name in pieces
    # Names = 8
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name03', message=True)    # Split the alternative name in pieces
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name04', message=True)    # Split the alternative name in pieces
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name05', message=True)    # Split the alternative name in pieces
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name06', message=True)    # Split the alternative name in pieces
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name07', message=True)    # Split the alternative name in pieces
    n_df08 = aarc_oa.format_authors_names(df = n_df08, var_name ='name08', message=True)    # Split the alternative name in pieces
    # Names = 9
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name03', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name04', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name05', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name06', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name07', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name08', message=True)    # Split the alternative name in pieces
    n_df09 = aarc_oa.format_authors_names(df = n_df09, var_name ='name09', message=True)    # Split the alternative name in pieces
    # Names = 10
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name01', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name02', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name03', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name04', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name05', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name06', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name07', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name08', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name09', message=True)    # Split the alternative name in pieces
    n_df10 = aarc_oa.format_authors_names(df = n_df10, var_name ='name10', message=True)    # Split the alternative name in pieces
    # Step 3. Get the alternative surname and run the tests 
    # Names = 1
    n_df01 = gen_surname_alternative_and_run_test(n_df01, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    # Names = 2
    n_df02 = gen_surname_alternative_and_run_test(n_df02, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df02 = gen_surname_alternative_and_run_test(n_df02, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    # Names = 3
    n_df03 = gen_surname_alternative_and_run_test(n_df03, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df03 = gen_surname_alternative_and_run_test(n_df03, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df03 = gen_surname_alternative_and_run_test(n_df03, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    # Names = 4
    n_df04 = gen_surname_alternative_and_run_test(n_df04, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df04 = gen_surname_alternative_and_run_test(n_df04, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df04 = gen_surname_alternative_and_run_test(n_df04, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df04 = gen_surname_alternative_and_run_test(n_df04, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    # Names = 5
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name05',num_alternatives=5) # Get the surname from the OpenAlex data
    # Names = 6
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    n_df05 = gen_surname_alternative_and_run_test(n_df05, var_name ='name05',num_alternatives=5) # Get the surname from the OpenAlex data
    # Names = 6
    n_df06 = gen_surname_alternative_and_run_test(n_df06, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df06 = gen_surname_alternative_and_run_test(n_df06, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df06 = gen_surname_alternative_and_run_test(n_df06, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df06 = gen_surname_alternative_and_run_test(n_df06, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    n_df06 = gen_surname_alternative_and_run_test(n_df06, var_name ='name05',num_alternatives=5) # Get the surname from the OpenAlex data
    n_df06 = gen_surname_alternative_and_run_test(n_df06, var_name ='name06',num_alternatives=6) # Get the surname from the OpenAlex data
    # Names = 7
    n_df07 = gen_surname_alternative_and_run_test(n_df07, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df07 = gen_surname_alternative_and_run_test(n_df07, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df07 = gen_surname_alternative_and_run_test(n_df07, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df07 = gen_surname_alternative_and_run_test(n_df07, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    n_df07 = gen_surname_alternative_and_run_test(n_df07, var_name ='name05',num_alternatives=5) # Get the surname from the OpenAlex data
    n_df07 = gen_surname_alternative_and_run_test(n_df07, var_name ='name06',num_alternatives=6) # Get the surname from the OpenAlex data
    n_df07 = gen_surname_alternative_and_run_test(n_df07, var_name ='name07',num_alternatives=7) # Get the surname from the OpenAlex data
    # Names = 8
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name05',num_alternatives=5) # Get the surname from the OpenAlex data
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name06',num_alternatives=6) # Get the surname from the OpenAlex data
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name07',num_alternatives=7) # Get the surname from the OpenAlex data
    n_df08 = gen_surname_alternative_and_run_test(n_df08, var_name ='name08',num_alternatives=8) # Get the surname from the OpenAlex data
    # Names = 9
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name05',num_alternatives=5) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name06',num_alternatives=6) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name07',num_alternatives=7) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name08',num_alternatives=8) # Get the surname from the OpenAlex data
    n_df09 = gen_surname_alternative_and_run_test(n_df09, var_name ='name09',num_alternatives=9) # Get the surname from the OpenAlex data
    # Names = 10
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name01',num_alternatives=1) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name02',num_alternatives=2) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name03',num_alternatives=3) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name04',num_alternatives=4) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name05',num_alternatives=5) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name06',num_alternatives=6) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name07',num_alternatives=7) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name08',num_alternatives=8) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name09',num_alternatives=9) # Get the surname from the OpenAlex data
    n_df10 = gen_surname_alternative_and_run_test(n_df10, var_name ='name10',num_alternatives=10) # Get the surname from the OpenAlex data
    # Step 4. Get a single variable that summarizes all the comparisons
    n_df01['SurnameComparison_tot'] = n_df01['SurnameComparison1']
    n_df02['SurnameComparison_tot'] = n_df02['SurnameComparison1'] + n_df02['SurnameComparison2']
    n_df03['SurnameComparison_tot'] = n_df03['SurnameComparison1'] + n_df03['SurnameComparison2'] + n_df03['SurnameComparison3']
    n_df04['SurnameComparison_tot'] = n_df04['SurnameComparison1'] + n_df04['SurnameComparison2'] + n_df04['SurnameComparison3'] + n_df04['SurnameComparison4']
    n_df05['SurnameComparison_tot'] = n_df05['SurnameComparison1'] + n_df05['SurnameComparison2'] + n_df05['SurnameComparison3'] + n_df05['SurnameComparison4'] + n_df05['SurnameComparison5']
    n_df06['SurnameComparison_tot'] = n_df06['SurnameComparison1'] + n_df06['SurnameComparison2'] + n_df06['SurnameComparison3'] + n_df06['SurnameComparison4'] + n_df06['SurnameComparison5'] + n_df06['SurnameComparison6']
    n_df07['SurnameComparison_tot'] = n_df07['SurnameComparison1'] + n_df07['SurnameComparison2'] + n_df07['SurnameComparison3'] + n_df07['SurnameComparison4'] + n_df07['SurnameComparison5'] + n_df07['SurnameComparison6'] + n_df07['SurnameComparison7']
    n_df08['SurnameComparison_tot'] = n_df08['SurnameComparison1'] + n_df08['SurnameComparison2'] + n_df08['SurnameComparison3'] + n_df08['SurnameComparison4'] + n_df08['SurnameComparison5'] + n_df08['SurnameComparison6'] + n_df08['SurnameComparison7'] + n_df08['SurnameComparison8']
    n_df09['SurnameComparison_tot'] = n_df09['SurnameComparison1'] + n_df09['SurnameComparison2'] + n_df09['SurnameComparison3'] + n_df09['SurnameComparison4'] + n_df09['SurnameComparison5'] + n_df09['SurnameComparison6'] + n_df09['SurnameComparison7'] + n_df09['SurnameComparison8'] + n_df09['SurnameComparison9']
    n_df10['SurnameComparison_tot'] = n_df10['SurnameComparison1'] + n_df10['SurnameComparison2'] + n_df10['SurnameComparison3'] + n_df10['SurnameComparison4'] + n_df10['SurnameComparison5'] + n_df10['SurnameComparison6'] + n_df10['SurnameComparison7'] + n_df10['SurnameComparison8'] + n_df10['SurnameComparison9'] + n_df10['SurnameComparison10']
    # Step 5. Format the dataframes to be able to merge them
    sel_cols = ['id', 'display_name', 'alternative_names_num',
       'display_name_alternatives', 'PersonId', 'PersonName', 'Gender',
       'SurnameComparison_tot']
    n_df01 = n_df01[sel_cols] # Select the relevant columns
    n_df02 = n_df02[sel_cols] # Select the relevant columns
    n_df03 = n_df03[sel_cols] # Select the relevant columns
    n_df04 = n_df04[sel_cols] # Select the relevant columns
    n_df05 = n_df05[sel_cols] # Select the relevant columns
    n_df06 = n_df06[sel_cols] # Select the relevant columns
    n_df07 = n_df07[sel_cols] # Select the relevant columns
    n_df08 = n_df08[sel_cols] # Select the relevant columns
    n_df09 = n_df09[sel_cols] # Select the relevant columns
    n_df10 = n_df10[sel_cols] # Select the relevant columns
    # Step 6. Concatenate the dataframes and return it to the user
    n_fdf = pd.concat([n_df01, n_df02, n_df03, n_df04,
                       n_df05, n_df06, n_df07, n_df08,
                       n_df09, n_df10], ignore_index=True) # Concatenate the dataframes
    return n_fdf # Return the final dataframe


y = gender_surname_comparison(n_df) # Run the function to get the final dataframe

# Filter women with SurnameComparison_tot > 0
y = y[(y['Gender'] == 'F') & (y['SurnameComparison_tot'] > 0)] # Filter the DataFrame to only include rows

y.shape[0]

fpath = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\gender_surname_test.xlsx"
y.to_excel(fpath, index=False) # Save the file to check the results




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

