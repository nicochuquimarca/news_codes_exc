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

# 12. Match Non-AARC authors (Scopus) with OpenAlex (Fabrizio request)
# Scopus API Key = "b4e05d3254be1d55b8e29dc0948d8a3a"
# Job: Build a scraper that searches by scopus author id
# An example: https://api.openalex.org/authors/scopus:6507960713


# 0. Open Fabrizio's file with the Scopus authors and do some minor formatting
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Data_for_Nicholas.csv"
df = pd.read_csv(fp) # Read the file
# 0.1 Prepare 'personid' column for any potential match
df['personid'] = df['personid'].fillna(0)
df['personid'] = df['personid'].astype(int)
df.rename(columns = {'personid':'PersonId'}, inplace = True)

# 1. Split the guys with a value distinct from 'nan' in the personid column
nn_df = df[df['PersonId'] != 0] # Filter the DataFrame to only include rows where the 'PersonId' column is not null
n_df = df[df['PersonId']  == 0] # Filter the DataFrame to only include rows where the 'PersonId' column is null

# 2. Work with the non-null personid DataFrame (get the OpenAlexId and the name)
fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_businessecon_dictionary.xlsx"
be_df = pd.read_excel(fp) # Read the BusinessEcon Faculty list
# 2.1 Do a merge to get the OpenAlexId and the name
nn_be_df = pd.merge(nn_df, be_df, on='PersonId', how='left') # Merge the two DataFrames on the 'PersonId' column
aarc_oa.check_duplicates_and_missing_values(original_df = nn_df, new_df = nn_be_df, column_name = 'PersonOpenAlexId', check_missing_values = False) # Check the duplicates and missing values

# 3. Work with the null personid DataFrame (get the OpenAlexId and the name)
tdf = n_df.head(50) 
tdf


# Examples
# An example: https://api.openalex.org/authors/scopus:6507960713
# 6507960713   peter rebiero
# 57207796740  gerald á¸§ubl    
# 7203065569   ellen peters     
# 6603360672   benedict dellaert
# 57208724089  daniel goldstein 
# 56228692400  steve bellman    
# 7402508587   john little      

# FnXX: get_openalex_id = Function to get the OpenAlexId and the name from a Scopus author id
def get_openalex_id(sid):
    # 1. Call the API
    surl         = 'https://api.openalex.org/authors/scopus:' + sid # Define the URL to call the API
    api_response = requests.get(surl)

    # 2 Check if the response is successful
    api_response_test = api_response.status_code

    # 3. If the response is not successful, return an empty dataframe
    if api_response_test != 200:
        api_found = 'No'
        df = pd.DataFrame({
        'ScopusAuthorId'    : [sid],
        'PersonOpenAlexId'  : [None],
        'PersonOpenAlexName': [None],
        'api_found'         : [api_found]
        })
        return df
    # 4. If the response is successful, continue 
    elif api_response_test == 200: 
      # 4.1 Get the data
      api_data = api_response.json() # Extract the JSON data from the response
      # 4.2 Transform from JSON to DataFrame
      api_found = 'Yes' # Set the api_found variable to 'Yes'
      oa_id   = api_data['id']
      oa_name = api_data['display_name']
      df = pd.DataFrame({
        'ScopusAuthorId'    : [sid],
        'PersonOpenAlexId'  : [oa_id],
        'PersonOpenAlexName': [oa_name],
        'api_found'         : [api_found]
        })
      # 4.3 Do some formatting
      df = aarc_oa.delete_links(df,['PersonOpenAlexId']) # Delete the links in the 'PersonOpenAlexId' column
      # 4.4 Return the DataFrame with the OpenAlexId and the name
      return df # Return the DataFrame with the OpenAlexId and the name

# FnXX: process_scopus_openalex_ids = Function that enable multiple workers to call the get_openalex_id function
def process_scopus_openalex_ids(sid):
    # 3 workers --> time sleep = 0.05
    # 4 workers --> time sleep = 0.20
    time.sleep(0.06) # Introduce a small delay between requests
    return get_openalex_id(sid)


# Fnxx: linear_scopus_openalex_scraper = Linear process to get the openalex id from a list of scopus author ids 
def linear_scopus_openalex_scraper(wd_path,sid_vec):
    # Linear process
    sid_list = []
    # Iterate over the sid_ids
    for current_iter, sid in enumerate(sid_vec, start=1):  # start=1 to start counting from 1
        # Get the df of the current scopus author id
        df_scopus_oa = get_openalex_id(sid)
        # Append the df to the list
        sid_list.append(df_scopus_oa)
    # Concatenate the list of dataframes
    linear_scopus_oa_df = pd.concat(sid_list, axis = 0)
    # Save the DataFrame
    # Get the dates to save the files
    current_time = datetime.now()
    date_string  = aarc_oa.date_time_string(current_time = current_time)
    path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\scopusid_openalexid\\scopusid_openalexid_"+date_string+".csv"
    linear_scopus_oa_df.to_csv(path, index = False)
    print("Author Scopus ID and OpenAlex ID information has been saved in the following path: ", path)
    # Return the DataFrame
    return linear_scopus_oa_df


# Fnxx: parallel_scopus_openalex_scraper = Parallel process to get the openalex id from a list of scopus author ids
def parallel_paper_scraper(wd_path,sid_vec):
    # Parallel process, limited to 3 workers due to API response constraints
    # Initialize an empty list to store the results
    sid_list = []
    
    # Use ThreadPoolExecutor to parallelize the API requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Map the process_paper_authors function to the doi_vec
        results = list(executor.map(process_scopus_openalex_ids, sid_vec))
        # Filter out None or empty DataFrames
        sid_list = [df for df in results if df is not None and not df.empty]
    
    # Concatenate the list of dataframes
    parallel_scopus_oa_df = pd.concat(sid_list, axis=0)
    current_time = datetime.now()
    date_string  = aarc_oa.date_time_string(current_time = current_time)
    path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\scopusid_openalexid\\scopusid_openalexid_"+date_string+".csv"
    parallel_scopus_oa_df.to_csv(path, index = False)
    print("Author Scopus ID and OpenAlex ID information has been saved in the following path: ", path)
    # Return the DataFrame
    return parallel_scopus_oa_df



# Test the linear process
sid_vec = ['6507960713','57207796740','7203065569','6603360672','57208724089','56228692400','7402508587']
z = linear_scopus_openalex_scraper(wd_path = wd_path, sid_vec = sid_vec) # Call the function to get the OpenAlexId and the name
z = parallel_paper_scraper(wd_path = wd_path, sid_vec = sid_vec) # Call the function to get the OpenAlexId and the name





# Test the single function
x = '6507960713'  # This works  
x = '057207796740' # This does not work
y = get_openalex_id(sid = x) # Call the function to get the OpenAlexId and the name

