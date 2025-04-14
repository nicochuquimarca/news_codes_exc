                                # Research in the Media
# Objectives: 1. Match the private data ids (AARC) with publicly available data ids (openalex)
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
# version 5.1: 2025-02-21: Get the author basic information to do an OpenAlex duplicates excercise
# version 6.1: 2025-02-23: Handle the duplicates and concatenate the manually searched OpenAlex authors
# version 6.3: 2025-02-25: Create a global match between the AARC and OpenAlex authors
# version 6.4: 2025-03-06: Connect to the 02_aarc_twitter_match_via_openalex.py file, clean the twitter data
# version 6.5: 2025-03-10: Continue building the BusinessEcon Faculty dictionary
# version 6.6: 2025-03-27: Continue with the BusinessEcon Faculty dictionary, but translate the merge to a function
# version 7.1: 2025-03-31: See how many people from the DOI files were not matched by the first exercise
# version 8.1: 2025-04-02: Get the OpenAlex information for the papers (title,date keywords, fields).
# version 8.2: 2025-04-04: Continue with the OpenAlex information for the papers (title,date keywords, fields).
# version 8.3: 2025-04-05: Continue with the OpenAlex information for the papers (title,date keywords, fields).
# version 8.4: 2025-04-13: Handle the duplicats from the second excercise of names matching
# version 8.5: 2025-04-14: Continue with the duplicates from the second excercise of names matching, move to functions for efficiency


# Function List
# Functions 38, 39, 40 and 41 where written by Bernhard Finke in the main.py file shared by slack.
# Fn01: format_dictionary                          = Format the institution dictionary to merge with the faculty list
# Fn02: check_duplicates_and_missing_values        = Check for duplicates and missing values in the merge
# Fn03: format_faculty                             = Replace Institutions without an OpenAlexId with the OpenAlexId of the parent institution
# Fn04: reorder_columns                            = Set a column in a specific position
# Fn05: get_paper_authors                          = Get the authors openalex ids and names from a given paper
# Fn06: get_paper_info                             = Get the metadata of a paper using the DOI
# Fn07: process_paper_authors                      = Function that enables multiple workers to call the get_paper_authors function
# Fn08: process_paper_info                         = Function that enable multiple workers to call the get_paper_info function
# Fn09: date_time_string                           = Get the current date and time in a string format
# Fn10: linear_papers_scraper                      = Linear process to get the paper data (authors or info) of a list of papers
# Fn11: parallel_papers_scraper                    = Parallel process to get the paper data (authors or info) of a list of papers
# Fn12: gen_final_papers_csv                       = Generate the aggregate papers data in a csv
# Fn13: gen_papers_doi_to_call                     = Generate the papers doi to call the API
# Fn14: generate_scrap_batches                     = Divide the Dataframe into the scrap batches
# Fn15: format_authors_names                       = Format the author names previous to match by doi-author_name
# Fn16. prepare_names_for_merge                    = Prepare the names for the merge, based on the number of components
# Fn17: merge_and_save_dfs                         = Merge the original and open Alex DataFrames and save the matches
# Fn18: get_authors_surnames                       = Get the surnames of the authors
# Fn19: gen_final_aarc_openalex_authors_dictionary = Generate the final aarc openalex authors file and the authors dictionary
# Fn20: get_pending_authors                        = Get the authors that have not been matched from previous iterations
# Fn21: doi_author_surname_match_excercise         = Get the OpenAlexId for the authors by matching surname within each paper (using the doi to match a paper with the authors)
# Fn22: fill_na_ycols_to_the_left                  = Fill the missing years to the left when creating a DataFrame
# Fn23: fill_na_ycols_to_the_right                 = Fill the missing years to the right when creating a DataFrame
# Fn24: handle_missing_years                       = Handle the missing years in the years vector
# Fn25: format_author_df                           = Format the final df the get_works_by_year function produces
# Fn26: gen_empty_author_df                        = Generate an empty DataFrame for authors that have no data (either not found by the API or deleted)
# Fn27: get_works_by_year                          = Get the works by year for a given author_id
# Fn28: process_author_id                          = Function that enable multiple workers to call the get_works_by_year function
# Fn29: linear_works_by_year                       = Linear process to get the works by year for a list of authors
# Fn30: parallel_works_by_year                     = Parallel process to get the works by year for a list of authors
# Fn31: gen_final_works_by_year_csv                = Generate the final works by year DataFrame
# Fn32: gen_authors_ids_to_call                    = Generate the authors ids to call the API
# Fn33: gen_final_data_with_oa_ids                 = Generate the final data with the OpenAlex ids
# Fn34: prepare_twitter_data                       = Prepare the twitter data for the twitter-openalex-aarc matching (1:1 matching)
# Fn35 use_aux_df                                  = Use the auxiliary file to know which authors to keep from the duplicates df
# Fn36 gen_aarc_openalex_dictionary                = Generate the AARC-OpenAlex dictionary depending on the type of dictionary (reduced vs full sample)
# Fn37: get_aarc_openalex_dictionary_progress      = Get the AARC-OpenAlex dictionary progress
# Fn38: fix_name                                   = Fix names by  capitalizing the first letter, and swapings the first and last names
# Fn39: remove_middle_name                         = Removes the middle names (keeps only first and last names)
# Fn40: get_last_name                              = Gets the last name of a string
# Fn41: bernhard_matching_procedure                = Match the DOI-Author name using the Bernhard procedure
# Fn42: format_bernhard_matches                    = Format the bernhard matches to be used to create the final dictionary


# 0. Packages in the source code file
import sys, os, pandas as pd, ast, requests, math # Import the regular packages
from datetime import date                         # Get the current date
from datetime import datetime                     # Get the current date and time
import concurrent.futures, string                 # For parallel processing
import glob, time                                 # To call elements in a folder and time measurement
from unidecode import unidecode                   # For string manipulation
import numpy as np                                # For numerical operations


# Fn01: format_dictionary = Format the institution dictionary to merge with the faculty list
def format_dictionary(df,format_type):
    if format_type == 'Institution':
        select_cols1 = ['aarc_id', 'id', 'display_name'] # Select the columns to collect on the merge
        df = df[select_cols1]                            # Filter the columns
        df.rename(columns = {'aarc_id':'InstitutionId', 'display_name':'InstitutionOpenAlexName'}, inplace = True) # Rename the column to merge
        df['InstitutionOpenAlexId'] = df['id'].apply(lambda x: x.split('/')[-1]) # Extract the openalex id
        select_cols2 = ['InstitutionId', 'InstitutionOpenAlexId', 'InstitutionOpenAlexName'] # Select the columns to collect on the merge
        df = df[select_cols2] # Filter the columns        
        df = df[df['InstitutionId'].notnull()] # Remove Caltech (has a null id)
        df['InstitutionId'] = df['InstitutionId'].astype('int64') # Convert the id to integer to match with the faculty list
        return df
    elif format_type == 'DegreeInstitution':
        select_cols1 = ['aarc_id', 'id', 'display_name'] # Select the columns to collect on the merge
        df = df[select_cols1]                            # Filter the columns
        df.rename(columns = {'aarc_id':'DegreeInstitutionID', 'display_name':'DegreeInstitutionOpenAlexName'}, inplace = True) # Rename the column to merge
        df['DegreeInstitutionOpenAlexId'] = df['id'].apply(lambda x: x.split('/')[-1]) # Extract the openalex id
        select_cols2 = ['DegreeInstitutionID', 'DegreeInstitutionOpenAlexId', 'DegreeInstitutionOpenAlexName'] # Select the columns to collect on the merge
        df = df[select_cols2] # Filter the columns        
        df = df[df['DegreeInstitutionID'].notnull()] # Remove Caltech (has a null id)
        df['DegreeInstitutionID'] = df['DegreeInstitutionID'].astype('int64') # Convert the id to integer to match with the faculty list
        return df
    else:
        print('Error: The format type is not valid')
        return None

# Fn02: check_duplicates_and_missing_values = Check for duplicates and missing values in the merge
def check_duplicates_and_missing_values(original_df,new_df,column_name,check_missing_values = True):
    original_nrows = original_df.shape[0] 
    new_nrows      = new_df.shape[0] 
    assert original_nrows == new_nrows, "Error: There are duplicates in the merge"
    # S2. Check for missing values in a specified column (optional)
    if check_missing_values == True:
        missing_values = new_df[column_name].isnull().sum()
        assert missing_values == 0, "Error: There are " + str(missing_values) + " missing values in the merge, please do some checks"
        # S3. S1 and S2 are correct, print a message and return None
        print("The merge is correct, there are no duplicates or missing values")
    elif check_missing_values == False:
        print("The merge is correct, there are no duplicates, missing values have not been checked")
    return None

# Fn03: format_faculty = Replace Institutions without an OpenAlexId with the OpenAlexId of the parent institution
def format_faculty(df,institution_var_name):
    # S.0 Cheat Sheet
    #  Id	  InstitutionName 		                       ParentId  ParentInstitutionName
    #  369	  Rutgers - Newark                        ---> 177       Rutgers, The State University of New Jersey			
    #  235	  University of Texas-Pan American, The   ---> 557985    The University of Texas Rio Grande Valley
    #  539130 University of Texas at Brownsville, The ---> 557985    The University of Texas Rio Grande Valley
    #  537408 Rutgers University-Camden	              ---> 177       Rutgers, The State University of New Jersey
    
    # S1. Replace InstitutionId 
    df.loc[df[institution_var_name] == 369, institution_var_name] = 177.0       # Rutgers - Newark ---> Rutgers, The State University of New Jersey
    df.loc[df[institution_var_name] == 235, institution_var_name] = 557985.0    # University of Texas-Pan American, The ---> The University of Texas Rio Grande Valley
    df.loc[df[institution_var_name] == 539130, institution_var_name] = 557985.0 # University of Texas at Brownsville, The ---> The University of Texas Rio Grande Valley
    df.loc[df[institution_var_name] == 537408, institution_var_name] = 177.0    # Rutgers University-Camden ---> Rutgers, The State University of New Jersey
    
    # S2. Return the formatted faculty
    return df

# Fn04: reorder_columns = Set a column in a specific position
def reorder_columns(df, column_name, position):
    cols = list(df.columns)
    cols.insert(position, cols.pop(cols.index(column_name)))
    df = df[cols]
    return df

# Fn05: get_paper_authors = Get the authors openalex ids and names from a given paper
def get_paper_authors(doi):
    # S.1 Call the API
    doi_url = 'https://api.openalex.org/works/https://doi.org/' + doi
    wapi_response = requests.get(doi_url)
    
    # S.2 Check if the response is successful
    wapi_response_test = wapi_response.status_code
    if wapi_response_test != 200:
        # If the response is not succesfull return an empty dataframe
        api_found = 'No' # Create a variable to store if the api call was successful or not
        df = pd.DataFrame({
        'paper_id':    [None],
        'paper_doi':   doi,
        'paper_title': [None],
        'paper_num_authors': [None],
        'paper_author_position': [None],
        'author_id': [None],
        'author_display_name': [None],
        'paper_raw_author_name': [None],
        'api_found': api_found 
        })
        return df
    elif wapi_response_test == 200:
        # If the response is succesfull continue with the extraction
        api_found = 'Yes'
        # S.3 Get the data
        wapi_data   = wapi_response.json()
        # S.3.1 Get article level relevant data
        num_authors = len(wapi_data['authorships']) # Get the number of authors
        data = [] # Create a list to store the extracted data
        # S.3.2 Handle scenarios when authorships is empty
        if num_authors == 0:
            data.append({
                'paper_id': wapi_data['id'],
                'paper_doi': doi,
                'paper_title': wapi_data['title'],
                'paper_num_authors': num_authors,
                'paper_author_position': [None],
                'author_id': [None],
                'author_display_name': [None],
                'paper_raw_author_name': [None],
                'api_found': api_found
            })
            # Concatenate the extracted data into a dataframe
            df = pd.DataFrame(data)
        elif num_authors > 0:
            # S.3.2 Extract the required information for each author
            for authorship in wapi_data['authorships']:
                data.append({
                'paper_id': wapi_data['id'],
                'paper_doi': doi,
                'paper_title': wapi_data['title'],
                'paper_num_authors': num_authors,
                'paper_author_position': authorship['author_position'],
                'author_id': authorship['author']['id'],
                'author_display_name': authorship['author']['display_name'],
                'paper_raw_author_name': authorship['raw_author_name'],
                'api_found': api_found
                })
            # S.3.3 Concatenate the extracted data into a dataframe
            df = pd.DataFrame(data)
            df['author_id'] = df['author_id'].apply(lambda x: x.split('/')[-1]) # Format the author id only when it is not None
        
        # S.4 Format the Ids in the dataframe
        df['paper_id'] = df['paper_id'].apply(lambda x: x.split('/')[-1])
        
        # S.5 Return the dataframe
        return df

# Fn06: get_paper_info = Get the metadata of a paper using the DOI
def get_paper_info(doi):
    #print("Getting the paper information for the DOI: ", doi)
    # S.1 Call the API
    doi_url = 'https://api.openalex.org/works/https://doi.org/' + doi
    wapi_response = requests.get(doi_url)
    
    # S.2 Check if the response is successful
    wapi_response_test = wapi_response.status_code
    
    if wapi_response_test != 200:
        # If the response is not succesfull return an empty dataframe
        api_found = 'No' # Create a variable to store if the api call was successful or not
        df = pd.DataFrame({
        'paper_id':    [None],
        'paper_doi':   doi,
        'paper_title': [None],
        'paper_num_authors': [None],
        'paper_author_position': [None],
        'author_id': [None],
        'author_display_name': [None],
        'paper_raw_author_name': [None],
        'api_found': api_found,
        'issn_l': [None],
        'issn': [None],
        'journal_name': [None],
        'journal_oa_id': [None],
        'journal_volume': [None],
        'journal_issue': [None],
        'paper_fpage': [None],
        'paper_lpage': [None],
        'paper_retracted': [None],
        'issn_vec': [None],
        'paper_topics_vec': [None],
        'paper_references_vec': [None]
    })
        return df
    elif wapi_response_test == 200:
        # If the response is succesfull continue with the extraction
        api_found = 'Yes'
        # S.3 Get the data
        wapi_data   = wapi_response.json()
        # S.3.1 Get article level relevant data (includes journal information)
        num_authors = len(wapi_data['authorships']) # Get the number of authors
        # S.3.1.1 Handle scenarios when the primary_location is empty
        if wapi_data['primary_location']['source'] is None:
            issn_l        = None
            issn          = None
            journal_name  = None
            journal_oa_id = None
            issn_vec      = None
        elif wapi_data['primary_location']['source'] is not None:
            issn_l = wapi_data['primary_location']['source']['issn_l']
            issn = wapi_data['primary_location']['source']['issn']
            journal_name = wapi_data['primary_location']['source']['display_name']
            journal_oa_id = wapi_data['primary_location']['source']['id']
            issn_vec = wapi_data['primary_location']['source']['issn']
        # 3.1.2. Continue with the extraction of the data
        journal_volume = wapi_data['biblio']['volume']
        journal_issue = wapi_data['biblio']['issue']
        paper_fpage = wapi_data['biblio']['first_page']
        paper_lpage = wapi_data['biblio']['last_page']
        paper_retracted = wapi_data['is_retracted']
        paper_topics_vec = wapi_data['topics'] # Get the topics of the paper
        paper_references_vec = wapi_data['referenced_works'] # Get the topics of the paper
        
        data = [] # Create a list to store the extracted data
        # S.3.2 Handle scenarios when authorships is empty
        if num_authors == 0:
            data.append({
                'paper_id': wapi_data['id'],
                'paper_doi': doi,
                'paper_title': wapi_data['title'],
                'paper_num_authors': num_authors,
                'paper_author_position': [None],
                'author_id': [None],
                'author_display_name': [None],
                'paper_raw_author_name': [None],
                'api_found': api_found,
                'issn_l': issn_l,
                'issn': issn,
                'journal_name': journal_name,
                'journal_oa_id': journal_oa_id,
                'journal_volume': journal_volume,
                'journal_issue': journal_issue,
                'paper_fpage': paper_fpage,
                'paper_lpage': paper_lpage,
                'paper_retracted': paper_retracted,
                'issn_vec': issn_vec,
                'paper_topics_vec': paper_topics_vec,
                'paper_references_vec': paper_references_vec
            })
            # Concatenate the extracted data into a dataframe
            df = pd.DataFrame(data)
        elif num_authors > 0:
            # S.3.2 Extract the required information for each author
            for authorship in wapi_data['authorships']:
                data.append({
                'paper_id': wapi_data['id'],
                'paper_doi': doi,
                'paper_title': wapi_data['title'],
                'paper_num_authors': num_authors,
                'paper_author_position': authorship['author_position'],
                'author_id': authorship['author']['id'],
                'author_display_name': authorship['author']['display_name'],
                'paper_raw_author_name': authorship['raw_author_name'],
                'api_found': api_found,
                'issn_l': issn_l,
                'issn': issn,
                'journal_name': journal_name,
                'journal_oa_id': journal_oa_id,
                'journal_volume': journal_volume,
                'journal_issue': journal_issue,
                'paper_fpage': paper_fpage,
                'paper_lpage': paper_lpage,
                'paper_retracted': paper_retracted,
                'issn_vec': issn_vec,
                'paper_topics_vec': paper_topics_vec,
                'paper_references_vec': paper_references_vec
                })
            # S.3.3 Concatenate the extracted data into a dataframe
            df = pd.DataFrame(data)
            df['author_id'] = df['author_id'].apply(lambda x: x.split('/')[-1]) # Format the author id only when it is not None
        
        # S.4 Format the Ids in the dataframe
        df['paper_id'] = df['paper_id'].apply(lambda x: x.split('/')[-1])
        # S.4.1 Format the journal id only when it is not None        
        if journal_oa_id is not None:
            df['journal_oa_id'] = df['journal_oa_id'].apply(lambda x: x.split('/')[-1])
        # S.5 Return the dataframe
        return df

# Fn07: process_paper_authors = Function that enable multiple workers to call the get_paper_authors function
def process_paper_authors(doi):
    # 3 workers --> time sleep = 0.05
    # 4 workers --> time sleep = 0.20
    time.sleep(0.06) # Introduce a small delay between requests
    return get_paper_authors(doi)

# Fn08: process_paper_info = Function that enable multiple workers to call the get_paper_info function
def process_paper_info(doi):
    # 3 workers --> time sleep = 0.05
    # 4 workers --> time sleep = 0.20
    time.sleep(0.06) # Introduce a small delay between requests
    return get_paper_info(doi)

# Fn09: date_time_string = Get the current date and time in a string format
def date_time_string(current_time):
    year   = str(current_time.year)
    month  = str(current_time.month)
    if len(month)==1:
        month = "0" + month
    day    = str(current_time.day)
    if len(day)==1:
        day = "0" + day
    hour   = str(current_time.hour)
    if len(hour)==1:
        hour = "0" + hour
    minute = str(current_time.minute)
    if len(minute)==1:
        minute = "0" + minute
    second = str(current_time.second)
    if len(second)==1:
        second = "0" + second
    date_string = year + "-" +  month + "-"  + day + "_" + hour + "." + minute + "." + second
    return date_string

# Fn10: linear_papers_scraper = Linear process to get the paper data (authors or info) of a list of papers
def linear_paper_scraper(wd_path,doi_vec,scrap_fn):
    # Linear process
    papers_list = []
    # Iterate over the author_ids
    for current_iter, doi in enumerate(doi_vec, start=1):  # start=1 to start counting from 1
        # Get the df of the current id
        if scrap_fn == 'paper_authors':
            df_paper = get_paper_authors(doi)
        if scrap_fn == 'paper_info':
            df_paper = get_paper_info(doi)
        # Append the df to the list
        papers_list.append(df_paper)
    # Concatenate the list of dataframes
    linear_papers_df = pd.concat(papers_list, axis = 0)
    # Save the DataFrame
    # Get the dates to save the files
    current_time = datetime.now()
    date_string  = date_time_string(current_time = current_time)
    if scrap_fn == 'paper_authors':
        path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors\\doi_papers_authors_"+date_string+".csv"
    if scrap_fn == 'paper_info':
        path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_info\\doi_papers_info_"+date_string+".csv"
    linear_papers_df.to_csv(path, index = False)
    print("Papers information has been saved in the following path: ", path)
    # Return the DataFrame
    return linear_papers_df

# Fn11: parallel_papers_scraper = Parallel process to get the paper data (authors or info) of a list of papers
def parallel_paper_scraper(wd_path,doi_vec,scrap_fn):
    # Parallel process, limited to 3 workers due to API response constraints
    # Initialize an empty list to store the results
    papers_list = []
    
    if scrap_fn == 'paper_authors':
        # Use ThreadPoolExecutor to parallelize the API requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Map the process_paper_authors function to the doi_vec
            results = list(executor.map(process_paper_authors, doi_vec))
        # Filter out None or empty DataFrames
        papers_list = [df for df in results if df is not None and not df.empty]
    elif scrap_fn == 'paper_info':
        # Use ThreadPoolExecutor to parallelize the API requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Map the process_paper_authors function to the doi_vec
            results = list(executor.map(process_paper_info, doi_vec))
        # Filter out None or empty DataFrames
        papers_list = [df for df in results if df is not None and not df.empty]
    # Concatenate the list of dataframes
    parallel_papers_df = pd.concat(papers_list, axis=0)
    current_time = datetime.now()
    date_string  = date_time_string(current_time = current_time)
    if scrap_fn == 'paper_authors':
        path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors\\doi_papers_authors_"+date_string+".csv"
    if scrap_fn == 'paper_info':
        path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_info\\doi_papers_info_"+date_string+".csv"
    parallel_papers_df.to_csv(path, index = False)
    print("Papers information has been saved in the following path: ", path)
    # Return the DataFrame
    return parallel_papers_df

# Fn12: gen_final_papers_csv = Generate the aggregate papers data in a csv
def gen_final_papers_csv(wd_path,scrap_fn):
    # 1. Get the path to the folder with the files
    if scrap_fn == 'paper_authors':
        folder_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors"
    elif scrap_fn == 'paper_info':
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
    # 5. Delete the paper_author_position and delete duplicates in general
    if scrap_fn == 'paper_authors':
        final_df = final_df.drop(columns = ['paper_author_position'])
    final_df = final_df.drop_duplicates()
    # 6. Save the final DataFrame
    if scrap_fn == 'paper_authors':
        final_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
    elif scrap_fn == 'paper_info':
        final_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_info_openalex.csv"
    final_df.to_csv(final_file_path, index = False)
    print("The 'doi_papers_authors_openalex.csv' file has been saved successfully")
    return final_df

# Fn13: gen_papers_doi_to_call = Generate the papers doi to call the API
def gen_papers_doi_to_call(wd_path,source,scrap_fn):
    # S.1 Open the input data provided by Moqi
    if source == "aarc":
        file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
        df_papers = pd.read_excel(file_path)
        # S.1.1 Select specific columns and remove duplicates
        sel_cols  = ['doi']
    elif source == "aarc_yusuf":
        file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\merged_AARC_DOI_Scopus_id_Yusuf.csv"
        df_papers = pd.read_csv(file_path)
        df_papers.rename(columns = {'DOI':'doi'}, inplace = True) # change the name to make it consistent with the next steps
    # S.1.1 Select specific columns and remove duplicates
    sel_cols  = ['doi']
    df_papers = df_papers[sel_cols]
    df_papers = df_papers.drop_duplicates()

    # S.2 Open the already scrapped data
    if scrap_fn == 'paper_authors':
        file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
    elif scrap_fn == 'paper_info':
        file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_info_openalex.csv"
    df_papers_authors = pd.read_csv(file_path)
    # S.2.1 Select specific columns, rename columns and prepare for the merge
    sel_cols  = ['paper_doi']
    df_papers_authors = df_papers_authors[sel_cols]
    df_papers_authors.rename(columns = {'paper_doi':'doi'}, inplace = True)
    df_papers_authors['dummy_col'] = 1

    # S.3 Merge the two DataFrames to get which ones have not been called
    merged_df = pd.merge(df_papers, df_papers_authors, on = "doi", how = "left")
    # S.3.1 Get rid of those observations with dummy_col = 1 in the merged_df
    merged_df = merged_df[merged_df['dummy_col'].isnull()]
    # S.3.2 Return only the ids to call
    merged_df = merged_df[['doi']]

    # S.4 Return the pending to scrap DataFrame
    return merged_df

# Fn14: generate_scrap_batches = Divide the Dataframe into the scrap batches
def generate_id_batches(df,batch_size):
    # Parameters
    num_rows = len(df)                  # Number of rows in the DataFrame
    num_batches = math.ceil(num_rows/batch_size) # Number of batches to scrap
    id_vector_list = []                # List to store the id vectors
    
    for i in range(num_batches):
        if i == num_batches-1:
            id_vector = df.iloc[i*batch_size:num_rows, 0]
        else:
            id_vector = df.iloc[i*batch_size:i*batch_size+batch_size, 0]
        id_vector_list.append(id_vector)
    # Print an informative message on how many batches are pending to scrap
    id_vector_len = len(id_vector_list)
    print(f"The DataFrame has been divided into {id_vector_len} batches")
    
    return id_vector_list

# Fn15: format_authors_names = Format the author names previous to match by doi-author_name
def format_authors_names(df,var_name,message=False):
    # S1. Split the 'author_display_name' column into multiple columns
    names_split = df[var_name].str.split(' ', expand=True)
    # S2. Convert all strings in the split columns to uppercase
    names_split = names_split.apply(lambda x: x.str.upper())
    # S3. Remove all punctuation points from the strings
    names_split = names_split.apply(lambda x: x.str.replace(f"[{string.punctuation}]", "", regex=True))
    # S.4 Rename the new columns
    num_cols = names_split.shape[1]
    names_split.columns = [f'{var_name}_{i+1}' for i in range(num_cols)] 
    if message == True:
        print(f'The max. number of name components in this dataset is {num_cols} components')
    # S.5 Count the number of non-None values in each row
    names_split[f'{var_name}_tot'] = names_split.notna().sum(axis=1)
    # S.6 Concatenate the new columns with the original DataFrame
    df = pd.concat([df, names_split], axis=1)
    # S.8 Return the formatted DataFrame
    return df

# Fn16: prepare_names_for_merge = Prepare the names for the merge, based on the number of components
def prepare_names_for_merge(df,type_df,var_name,num_c,format_doi=False):
    # S1. Filter by number of components
    df = df[df[f'{var_name}_tot']==num_c]
    # S2. Rename the doi column if specified by the user
    if format_doi == True:
        df.rename(columns = {'paper_doi':'doi'}, inplace = True)
    # S2. Concatenate the name components into a single string
    #     Is done manually due to the nature of the datasets (first df is surname-name and second df is name-surname)
    # S2.1 Two components and original dataset
    if num_c == 2 and type_df == 'original':
        df['key'] = df['doi'] + '-' + df[f'{var_name}_1'] + '-' + df[f'{var_name}_2']
    elif num_c == 2 and type_df == 'openalex':
        df['key'] = df['doi'] + '-' + df[f'{var_name}_2'] + '-' + df[f'{var_name}_1']

    # S3. Select the columns to return
    if type_df == 'original':
        sel_cols = ['key','doi','aarc_personid','aarc_name']
        df       = df[sel_cols]
        df['aarc_personid'] = df['aarc_personid'].astype(str)
    elif type_df == 'openalex':
        sel_cols = ['key','doi','author_id','author_display_name','paper_raw_author_name','paper_num_authors']
        df       = df[sel_cols]
        df['paper_num_authors'] = df['paper_num_authors'].astype(str)
 
    # S.4 Remove accents from the strings
    df = df.applymap(unidecode)
    
    # S.5 Remove duplicated keys
    df['key_count'] = df.groupby('key')['key'].transform('count')
    keys_count = df['key_count'].unique()
    print(f'The unique key counts are: {keys_count}, I delete all the keys with more than one count')
    df = df[df['key_count'] == 1]
    df = df.drop(columns=['key_count'])

    # S.6 Return the formatted DataFrame
    return df

# Fn17: merge_and_save_dfs = Merge the original and open Alex DataFrames and save the matches
def merge_and_save_dfs(df1,df2,num_components):
    # S.1 Merge the dataframes
    df = pd.merge(df1, df2, on = 'key', how = 'left')
    # S.2 Filter the matches only
    df = df[df['author_id'].notnull()]
    # S.3 See how many matches were found
    num_matches = df.shape[0]
    assert num_matches > 0, "Error: No matches were found, please check the data"
    print(f'The number of matches found is {num_matches}')
    # S.3 Save the DataFrame
    path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\aarc_oa_authors_matches_name_components_" + num_components + ".csv" 
    df.to_csv(path, index = False)
    print("Papers information has been saved succesfully")
    # S.4 Return the DataFrame
    return df

# Fn18: get_authors_surnames = Get the surnames of the authors
def filter_df_and_test_surnames(df):
    # 1. Get relevant columns and remove duplicates
    df.rename(columns = {'aarc_personid':'PersonId','aarc_name':'PersonName',
                          'author_id':'PersonOpenAlexId','author_display_name':'PersonOpenAlexName'}, inplace = True)
    df = df[['PersonId','PersonName','PersonOpenAlexId','PersonOpenAlexName']]
    df = df.drop_duplicates()
    # 2. Get the surnames
    df = format_authors_names(df = df, var_name ='PersonName', message=False) # Split the names in pieces
    df.rename(columns = {'PersonName_1':'PersonSurname'}, inplace = True) # Rename the column that contains the surname
    df = format_authors_names(df = df, var_name ='PersonOpenAlexName', message=False) # Split the names in pieces
    df['PersonSurnameOpenAlex'] = df.apply(
        lambda row: row[f'PersonOpenAlexName_{row["PersonOpenAlexName_tot"]}'], axis=1) # Get the surname from the OpenAlex data
    df = df[['PersonId','PersonName','PersonOpenAlexId','PersonOpenAlexName','PersonSurname','PersonSurnameOpenAlex']] # Select only relevant columns
    df['PersonSurnameOpenAlex'] = df['PersonSurnameOpenAlex'].apply(unidecode) # Remove accents
    df['PersonSurnameOpenAlex'] = df['PersonSurnameOpenAlex'].str.replace('-', '') # Remove hyphens
    df['SurnameMatch'] = df['PersonSurname'] == df['PersonSurnameOpenAlex']
    # 3. Return the DataFrame
    return df

# Fn19: gen_final_aarc_openalex_authors_dictionary = Generate the final aarc openalex authors file and the authors dictionary
def gen_final_aarc_openalex_authors_dictionary(wd_path):
    folder_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches"
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
    matches_df = pd.concat(dfs, ignore_index=True)
    # 5. Clean the matches DataFrame (select columns, and convert to strings)
    matches_df = matches_df[['PersonId', 'PersonName', 'PersonOpenAlexId', 'PersonOpenAlexName']]
    matches_df['PersonId'] = matches_df['PersonId'].astype(str)
    # 6. Save the DataFrame
    matches_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches.csv"
    matches_df.to_csv(matches_file_path, index = False)
    print("The 'aarc_openalex_authors_matches.csv' has been saved successfully")
    # Now we will create a dictionary that tells me the authors that have been and have not been matched
    # 7. Open the Data of the authors I need to match
    # 7.1 USA part
    fp0 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\final_data_usa.csv"
    df0 = pd.read_csv(fp0)
    df0 = df0[['PersonId']]
    df0.drop_duplicates(inplace = True)
    df0['PersonId'] = df0['PersonId'].astype(str)
    # 7.2 Non-USA part
    fp1 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\final_data_nonusa.csv"
    df1 = pd.read_csv(fp1)
    df1 = df1[['PersonId']]
    df1.drop_duplicates(inplace = True)
    df1['PersonId'] = df1['PersonId'].astype(str)
    # 7.3 Concatenate them vertically and delete duplicates
    df_ids = pd.concat([df0,df1], axis = 0)
    df_ids.drop_duplicates(inplace = True)
    # 7.4 Merge with the AARC_people_DOI.xlsx and BusinessEconFacultyLists.csv files to recover the names
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
    df_names0 = pd.read_excel(fp)
    df_names0 = df_names0[['aarc_personid','aarc_name']]
    df_names0.rename(columns = {'aarc_personid':'PersonId', 'aarc_name':'PersonName'}, inplace = True) # Rename the column to merge
    df_names0['PersonId'] = df_names0['PersonId'].astype(str)
    df_names0.drop_duplicates(inplace = True)
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\BusinessEconFacultyLists.csv"
    df_names1 = pd.read_csv(fp)
    df_names1 = df_names1[['PersonId','PersonName']]
    df_names1['PersonId'] = df_names1['PersonId'].astype(str)
    df_names1.drop_duplicates(inplace = True)
    # 7.5 Handle some special cases that have a different name despite sharing the same PersonId
    df_names1.loc[df_names1['PersonId'] == '115042', 'PersonName'] = 'WILCOXEN, PETER' # The other name was 'WILCOXEN, PETER J'
    df_names1.loc[df_names1['PersonId'] == '115553', 'PersonName'] = 'PEARCE, DOUGLAS' # The other name was 'PEARCE, DOUGLAS K'
    df_names1.loc[df_names1['PersonId'] == '200379', 'PersonName'] = 'MUNNELL, ALICIA' # The other name was 'MUNNELL, ALICIA H'
    df_names1.drop_duplicates(inplace = True)
    # 7.6 Concatenate the two DataFrames and delete duplicates
    df_names = pd.concat([df_names0,df_names1], axis = 0)
    df_names.drop_duplicates(inplace = True)
    # 7.7 Merge the ids with the names
    df = pd.merge(df_ids, df_names, on = 'PersonId', how = 'left')
    # 8. Remove PersonName to avoid _x and _y columns
    matches_df = matches_df[['PersonId','PersonOpenAlexId', 'PersonOpenAlexName']]
    # 9. Merge the two DataFrames
    df2 = pd.merge(df, matches_df, on = 'PersonId', how = 'left')
    df2['matched'] = df2['PersonOpenAlexId'].notnull()
    # 10. Save the final DataFrame
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_dictionary.xlsx"
    df2.to_excel(file_path, index = False)
    print("The 'aarc_openalex_author_intermediate_dictionary.xlsx' been saved successfully")
    # 11. Create a Intermediate EconBusinessFaculty dictionary by re-using the dfnames1 df (df_names1--> Contains the econfaculty list, matches_df--> Contains the matches)
    df3 = pd.merge(df_names1, matches_df, on = 'PersonId', how = 'left')
    df3['matched'] = df3['PersonOpenAlexId'].notnull()
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_businessecon_dictionary.xlsx"
    df3.to_excel(file_path, index = False)
    print("The 'aarc_openalex_author_intermediate_businessecon_dictionary.xlsx' been saved successfully")
    # 12. Return the final DataFrame
    return df2

# Fn20: get_pending_authors = Get the authors that have not been matched from previous iterations
def get_pending_authors(wd_path,df):
    # 1. Open the final dictionary
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_dictionary.xlsx"
    ddf = pd.read_excel(fp)
    ddf = ddf[['PersonId','matched']]
    ddf = ddf.drop_duplicates()
    # 2. Do a merge with the two_author_papers_id_df
    ddf['PersonId'] = ddf['PersonId'].astype(str)
    df['PersonId'] = df['PersonId'].astype(str)   
    df2 = pd.merge(df, ddf, on = 'PersonId', how = 'left')
    # 3. Filter observations that meet the condition 'matched' == False
    df2 = df2[df2['matched']==False]
    df2 = df2.drop(columns = ['matched'])
    # 4. Return the final DataFrame|
    return df2

# Fn21: doi_author_surname_match_excercise = Get the OpenAlexId for the authors by matching surname within each paper (using the doi to match a paper with the authors)
def doi_author_surname_match_excercise(wd_path):
    # Step 1: Set a list of papers with doi from the original data and the OpenAlex data
    # 1.1 Original data (provided by Moqi)
    fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx" # File path
    df1 = pd.read_excel(fp)                                                                # Read the file
    df1 = df1[['aarc_personid','doi','aarc_name']] # Select the most basic columns
    df1 = df1.drop_duplicates()                    # Drop duplicates
    # 1.2 OpenAlex data
    fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
    df2 = pd.read_csv(fp)
    df2 = df2[df2['api_found']=="Yes"]           # Keep only papers with information from OpenAlex
    df2 = df2[['paper_doi','paper_num_authors']] # Select the most basic columns
    df2 = df2.drop_duplicates()                  # Drop duplicates
    # 1.3 Merge and create the dataframe to iterate over the number of authors in the papers
    df3 = pd.merge(df1, df2, left_on = 'doi', right_on = 'paper_doi', how = 'left')
    # 1.4 Open the OpenAlex data again and get more details about the authors
    fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
    df4 = pd.read_csv(fp)
    df4 = df4[['paper_doi','author_id','author_display_name','paper_raw_author_name']] 
    df4 = df4.drop_duplicates()

    # Step 2: Delete the files created in each iteration
    # Specify the folder path
    folder_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches"
    # Get a list of all files in the folder
    files = glob.glob(os.path.join(folder_path, "*"))
    # Delete each file in the folder
    for file in files:
        try:
            os.remove(file)
        except FileNotFoundError:
            pass
    # Delete the aarc_openalex_author_dictionary.xlsx, aarc_openalex_author_intermediate_businessecon_dictionary.xlsx and aarc_openalex_authors_matches.csv files since they are they are the final products created in each iteration
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_dictionary.xlsx"
    try:
        os.remove(fp) # Delete the file
    except FileNotFoundError:
        pass # If the file is not found, do nothing
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_intermediate_businessecon_dictionary.xlsx"
    try:
        os.remove(fp) # Delete the file
    except FileNotFoundError:
        pass # If the file is not found, do nothing
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches.csv"
    try:
        os.remove(fp) # Delete the file
    except FileNotFoundError:
        pass # If the file is not found, do nothing
    
    
    # Step 3: Start the matching exercise, the process will be done in iterations
    # 3.1 Iteration 1: Papers with one author only, it is outside of the loop because it is a special case
    iter1_df          = df3[df3['paper_num_authors']==1] # Papers with one author only
    iter1_id_df       = pd.merge(iter1_df, df4, on = 'paper_doi', how = 'left') # Step1: Merge by doi to get the authors information   
    iter1_id_df       = filter_df_and_test_surnames(iter1_id_df)        # Step2: Select columns and do a surname match test
    iter1_id_match_df = iter1_id_df[iter1_id_df['SurnameMatch']==True]          # Step3: Keep only the surnames that match
    num_matches       = iter1_id_match_df.shape[0]                              #        Number of matches
    print(f"Number of matches in iteration 1: {num_matches}")                   #        Print the number of matches
    file_path         = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_1_author_match.csv"
    iter1_id_match_df.to_csv(file_path, index = False)                          # Step4: Save the iteration 1 file
    x   = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path) # Step5: Generate the final files
    print("Iteration 1 completed")
    # 3.2 Iteration 2 up until there are no matches
    # Initialize the iteration counter
    i = 2
    # Run the loop until num_matches reaches 0 
    while True:
        print(f"Starting iteration {i}")
        iter_df    = df3[df3['paper_num_authors']==i]                               # Papers with i authors only
        iter_id_df = pd.merge(iter_df, df4, on = 'paper_doi', how = 'left')         # Step1: Merge by doi to get the authors information 
        iter_id_df = filter_df_and_test_surnames(iter_id_df)                # Step2: Select columns and do a surname match test
        iter_id_df = get_pending_authors(wd_path = wd_path, df =iter_id_df) # Step3: Get the authors that have not been matched from previous iterations
        iter_id_match_df = iter_id_df[iter_id_df['SurnameMatch']==True]             # Step4: Keep only the surnames that match
        iter_id_match_df = iter_id_match_df.drop_duplicates()                       #        Remove duplicates before saving
        num_matches_prev_iter = num_matches
        num_matches      = iter_id_match_df.shape[0]                                #        Number of matches
        print(f"Number of matches in iteration {i}: {num_matches}")                 #        Print the number of matches
        fp = wd_path + f"\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_{i}_author_match.csv"
        iter_id_match_df.to_csv(fp, index = False)                                  # Step5: Save the iteration 1 file
        x = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path)   # Step6: Generate the final files
        # Break the loop if num_matches is 0
        if num_matches == 0 and num_matches_prev_iter == 0:
            print("Two consecutive iterations with 0 matches, stopping the process")
            break
        # Increment the iteration counter
        i += 1
    return x

# Fn22: fill_na_ycols_to_the_left = Fill the missing years to the left when creating a DataFrame
def fill_na_ycols_to_the_left(df,max_year):
    num_lfill_columns = 2025 - max_year
    lfill_column_names = ['y' + str(max_year + 1 + i) for i in range(num_lfill_columns)]
    # Add the new columns with None values
    for col in lfill_column_names:
        df[col] = None
    # Return the DataFrame
    return df

# Fn23: fill_na_ycols_to_the_right = Fill the missing years to the right when creating a DataFrame
def fill_na_ycols_to_the_right(df,min_year):
    num_rfill_columns = min_year - 2012
    rfill_column_names = ['y' + str(min_year - 1 - i) for i in range(num_rfill_columns)]
    # Add the new columns with None values
    for col in rfill_column_names:
        df[col] = None
    # Return the DataFrame
    return df

# Fn24: handle_missing_years = Handle the missing years in the years vector
def handle_missing_years(years,works_counts,max_year,min_year):
    # Check wich years are missing in the years vector
    all_years_set = set(range(min_year, max_year +1)) # Set of all years
    emp_years_set = set(years)                        # Set of empirical years
    missing_years = all_years_set - emp_years_set     # Set difference
    missing_years = list(missing_years)               # Convert to list

    # Generate the dataframe with the years as columns
    # Create a DataFrame
    df = pd.DataFrame({
        'year': years,
        'works_count': works_counts
    })
    # Create a new dataframe with the missing years
    df_missing = pd.DataFrame({
        'year': missing_years,
        'works_count': [0]*len(missing_years)   
    })
    # Now concatenate the two DataFrames and sort the years in descending order
    df = pd.concat([df, df_missing], axis = 0)
    df = df.sort_values(by = 'year', ascending = False)
    # Transform back to vectors the years and works_counts
    years_new = df['year'].values
    works_counts_new = df['works_count'].values
    # Return the two vectors
    return years_new, works_counts_new

# Fn25: format_author_df = Format the final df the get_works_by_year function produces
def format_author_df(df,aid_id,aid_display_name,author_id,aid_works_count):
    # Add some extra columns to the dataframe
    df['author_id'] = aid_id
    df['author_display_name'] = aid_display_name
    df['author_id_API'] = author_id
    df['author_work_count'] = aid_works_count
    # Reorder the DataFrame in a desired way
    desired_order = ['author_id', 'author_display_name', 'author_id_API','author_work_count'] # Put this columns into the first positions
    remaining_columns = [col for col in df.columns if col not in desired_order] # Get the remaining columns
    new_order = desired_order + remaining_columns # Combine the desired order with the remaining columns
    df = df[new_order] # Reorder the DataFrame
    return df

# Fn26: gen_empty_author_df = Generate an empty DataFrame for authors that have no data (either not found by the API or deleted)
def gen_empty_author_df(aid_id,aid_display_name,author_id,aid_works_count):
    # Define and return an empty dataframe
    df = pd.DataFrame({
        'y2025': [None],'y2024': [None],'y2023': [None],'y2022': [None],
        'y2021': [None],'y2020': [None],'y2019': [None],'y2018': [None],
        'y2017': [None],'y2016': [None],'y2015': [None],'y2014': [None],
        'y2013': [None],'y2012': [None]
    })
    df = format_author_df(df,aid_id,aid_display_name,author_id,aid_works_count)
    return df

# Fn27: get_works_by_year = Get the works by year for a given author_id
def get_works_by_year(author_id):
    # Call the API
    aid_response = requests.get(author_id)
    # Check if the response is successful
    aid_response_test = aid_response.status_code
    if aid_response_test != 200:
        # If the response is not successful return an empty DataFrame
        aid_display_name = None
        extract_id = lambda author_id: author_id.split('/')[-1]
        idnum = extract_id(author_id) # Use the lambda function
        aid_id = 'https://openalex.org/' + idnum
        aid_works_count = None
        df = gen_empty_author_df(aid_id,aid_display_name,author_id,aid_works_count)
        return df
    elif aid_response_test == 200:
        # Get the data
        aid_data  = aid_response.json()
        aid_counts_b_year = aid_data.get('counts_by_year',[])
        aid_display_name  = aid_data.get('display_name')
        aid_id = aid_data.get('id')
        aid_works_count   = aid_data.get('works_count')        
        if aid_display_name == 'Deleted Author':
            # If the response is not successful return an empty DataFrame
            aid_display_name = 'Deleted Author'
            extract_id = lambda author_id: author_id.split('/')[-1]
            idnum = extract_id(author_id) # Use the lambda function
            aid_id = 'https://openalex.org/' + idnum
            aid_works_count = None
            df = gen_empty_author_df(aid_id,aid_display_name,author_id,aid_works_count)
            return df
        if not aid_counts_b_year:
            # If the aid_counts_b_year is empty return an empty DataFrame
            extract_id = lambda author_id: author_id.split('/')[-1]
            idnum = extract_id(author_id) # Use the lambda function
            aid_id = 'https://openalex.org/' + idnum
            df = gen_empty_author_df(aid_id,aid_display_name,author_id,aid_works_count)
            return df
        else: 
            # Extract years, works counts and cited by counts in different vectors
            years = [entry['year'] for entry in aid_counts_b_year]
            works_counts = [entry['works_count'] for entry in aid_counts_b_year]
            cited_by_counts = [entry['cited_by_count'] for entry in aid_counts_b_year]
            # Get the max year recorded for that author
            max_year = max(years)
            min_year = min(years)
            exp_num_columns = max_year - min_year + 1 # Expected number of columns
            emp_num_columns = len(years)              # Empirical number of columns
            # Check if the number of columns is different from the expected number of columns
            if exp_num_columns != emp_num_columns:
                # Handle the missing years
                years_new, works_counts_new = handle_missing_years(years,works_counts,max_year,min_year)
                works_counts = works_counts_new
                num_columns = len(years_new) # Number of columns after handling missing years
            else:
                years_new = years
                num_columns = len(years_new) # Number of columns after handling missing years
            # Create the base DataFrame
            num_columns = len(years_new) # Number of columns after handling missing years
            column_names = ['y' + str(max_year - i) for i in range(num_columns)]
            df = pd.DataFrame([works_counts], columns=column_names)
            # Handle complete or incomplete datasets
            if num_columns == 14 and max_year == 2025 and min_year == 2012:
                # Complete dataset, do nothing
                None
            elif num_columns < 14 and max_year == 2025 and min_year > 2012:
                # Fill to the right
                df = fill_na_ycols_to_the_right(df,min_year)
            elif num_columns < 14 and max_year < 2025 and min_year == 2012:
                # Fill to the left"
                df = fill_na_ycols_to_the_left(df,max_year)
            elif num_columns < 14 and max_year < 2025 and min_year > 2012:
                # Fill both sides
                df = fill_na_ycols_to_the_left(df,max_year)
                df = fill_na_ycols_to_the_right(df,min_year)
            # Sort the columns names in desceding order
            df = df.reindex(sorted(df.columns, reverse=True), axis=1)
            df = format_author_df(df,aid_id,aid_display_name,author_id,aid_works_count)
            return df

# Fn28: process_author_id = Function that enable multiple workers to call the get_works_by_year function
def process_author_id(author_id):
    # 3 workers --> time sleep = 0.05
    # 4 workers --> time sleep = 0.20
    time.sleep(0.06) # Introduce a small delay between requests
    return get_works_by_year(author_id)

# Fn29: linear_works_by_year = Linear process to get the works by year for a list of authors
def linear_works_by_year(wd_path,authors_vec):
    # Linear process
    authors_list = []
    # Iterate over the author_ids
    for current_iter, id in enumerate(authors_vec, start=1):  # start=1 to start counting from 1
        # Get the df of the current id
        df_author_id = get_works_by_year(id)
        # Append the df to the list
        authors_list.append(df_author_id)
    # Concatenate the list of dataframes
    linear_authors_df = pd.concat(authors_list, axis = 0)
    # Save the DataFrame
    # Get the dates to save the files
    current_time = datetime.now()
    date_string  = date_time_string(current_time = current_time)
    path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\openalex_authorids_dfs\\authors_works_by_y_"+date_string+".csv"
    linear_authors_df.to_csv(path, index = False)
    print("Authors works by year have been saved in the following path: ", path)
    # Return the DataFrame
    return linear_authors_df

# Fn30: parallel_works_by_year = Parallel process to get the works by year for a list of authors
def parallel_works_by_year(wd_path,authors_vec):
    # Parallel process, limited to 3 workers due to API response constraints
    # Initialize an empty list to store the results
    authors_list = []
    # Use ThreadPoolExecutor to parallelize the API requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Map the process_author_id function to the authors_vec
        results = list(executor.map(process_author_id, authors_vec))
    # Filter out None or empty DataFrames
    authors_list = [df for df in results if df is not None and not df.empty]
    # Concatenate the list of dataframes
    parallel_authors_df = pd.concat(authors_list, axis=0)
    current_time = datetime.now()
    date_string  = date_time_string(current_time = current_time)
    path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\openalex_authorids_dfs\\authors_works_by_y_"+date_string+".csv"
    parallel_authors_df.to_csv(path, index = False)
    print("Authors works by year have been saved in the following path: ", path)
    # Return the DataFrame
    return parallel_authors_df

# Fn31: gen_final_works_by_year_csv = Generate the final works by year DataFrame
def gen_final_works_by_year_csv(wd_path):
    folder_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\openalex_authorids_dfs"
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
    final_df = final_df.drop_duplicates()
    # 5. Save the final DataFrame
    final_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\openalex_authorsids_names_and_works.csv"
    final_df.to_csv(final_file_path, index = False)
    print("The authors works has been saved succesfully")
    return final_df

# Fn32: gen_authors_ids_to_call = Generate the authors ids to call the API
def gen_authors_ids_to_call(wd_path,dictionary_type):
    # Open the intermediate dictionary (user dependent)
    if dictionary_type == 'aarc_openalex_author_intermediate_dictionary':
        file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_dictionary.xlsx"
    if dictionary_type == 'aarc_openalex_author_intermediate_businessecon_dictionary':
        file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_businessecon_dictionary.xlsx"
    if dictionary_type == 'aarc_openalex_authors_bernhard_procedure':
        file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_bernhard_procedure.xlsx"
    df_authors = pd.read_excel(file_path)
    
    df_authors['PersonId_dups'] = df_authors['PersonId'].map(df_authors['PersonId'].value_counts())
    df_authors = df_authors[df_authors['PersonId_dups'] > 1]
    df_authors['author_id_API'] = "https://api.openalex.org/authors/" + df_authors['PersonOpenAlexId']
    # Select specific columns
    sel_cols = ['author_id_API']
    df_authors = df_authors[sel_cols]
    
    # Open the final_df to check if the id is already in the final_df
    fdf_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\openalex_authorsids_names_and_works.csv"
    fdf = pd.read_csv(fdf_path)
    fdf = fdf[['author_id_API']]
    fdf['dummy_col'] = 1
    
    # Merge the two DataFrames to get which ones have not been called
    merged_df = pd.merge(df_authors, fdf, on = "author_id_API", how = "left")

    # Get rid of those observations with dummy_col = 1 in the merged_df
    merged_df = merged_df[merged_df['dummy_col'].isnull()]

    # Return only the ids to call
    merged_df = merged_df[['author_id_API']]

    # Return the pending to scrap DataFrame
    return merged_df

# Fn33: gen_final_data_with_oa_ids = Generate the final data with the OpenAlex ids
def gen_final_data_with_oa_ids(wd_path,file_name):
    # 5.1 Open the data files
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\"+file_name+".csv"
    df = pd.read_csv(fp)    
    df['PersonId'] = df['PersonId'].astype(str)
    # 5.2 Open the final authors dictionary
    dict_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_dictionary.xlsx"
    dict_df = pd.read_excel(dict_fp)
    dict_df['PersonId'] = dict_df['PersonId'].astype(str)
    dict_df = dict_df.drop(columns=['AutoMatch']) # Drop the matched column
    # 5.3 Merge the data with the dictionary and do a check
    oa_authorid_df = pd.merge(df, dict_df, on = "PersonId", how = "left") # Merge with the dictionary
    check_duplicates_and_missing_values(original_df = df, new_df = oa_authorid_df, column_name='PersonOpenAlexId')
    # 5.4 Reorder columns for export purposes
    oa_authorid_df = reorder_columns(df = oa_authorid_df, column_name = 'PersonOpenAlexId', position = 1)
    oa_authorid_df = reorder_columns(df = oa_authorid_df, column_name = 'PersonName', position = 8)
    oa_authorid_df = reorder_columns(df = oa_authorid_df, column_name = 'PersonOpenAlexName', position = 9)
    # 5.6 Merge the data from the Institutions
    inst_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Final_sample_AARC_IPEDS_filtered_with_id.csv"
    inst_df = pd.read_csv(inst_fp)
    inst1_df = format_dictionary(inst_df,'Institution')
    oa_authorid_format_df = format_faculty(oa_authorid_df,'InstitutionId') # Format the faculty
    oa_authorid_inst_df = pd.merge(oa_authorid_format_df, inst1_df, on = "InstitutionId", how = "left") # Merge with the institution
    check_duplicates_and_missing_values(original_df = oa_authorid_format_df, new_df = oa_authorid_inst_df, column_name='InstitutionOpenAlexId')
    # 5.7 Reorder columns for export purposes
    oa_authorid_inst_df = reorder_columns(df = oa_authorid_inst_df, column_name = 'InstitutionOpenAlexId', position = 3)
    oa_authorid_inst_df = reorder_columns(df = oa_authorid_inst_df, column_name = 'InstitutionOpenAlexName', position = 5)
    # 5.8 Save the final data
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\"+ file_name + "_OpenAlexIds.csv"
    oa_authorid_inst_df.to_csv(file_path, index = False)
    print("The file was saved in: ", file_path)
    return oa_authorid_inst_df
        
# Fn34: prepare_twitter_data = Prepare the twitter data for the twitter-openalex-aarc matching (1:1 matching)
def prepare_twitter_data(wd_path):
    # 1. Load the twitter data
    fp = wd_path + "\\data\\raw\\twitter_openalex\\input_files\\authors_tweeters_2024_02.csv"
    t_df = pd.read_csv(fp)
    
    # 2. Clean the df and check for duplicates
    t_df['OpenAlexId'] = t_df['author_id'].apply(lambda x: x.split('/')[-1]) # Extract the openalex id
    # Database author notes:  'The "alternative" column indicates if the match was made with the primary name (0) or an alternate name (1).'
    t_df.rename(columns={'tweeter_id': 'TwitterId'}, inplace=True) # The proper name is Twitter not Tweeter
    t_df.rename(columns={'author_id': 'OpenAlexLink'}, inplace=True)
    t_df['MatchingMethod'] = t_df['alternative'].apply(lambda x: 'primary name' if x == 0 else 'alternate name')
    t_df = t_df.drop(columns=['alternative'])
    t_df['TwitterId_dups'] = t_df['TwitterId'].map(t_df['TwitterId'].value_counts()) # Count the duplicates
    
    # 3. Divide the df into duplicates and non-duplicates (based on the TwitterId) and save the fuzzy data in a folder
    t_df_dups   = t_df[t_df['TwitterId_dups'] > 1]  # Get the duplicates
    t_df_ndups  = t_df[t_df['TwitterId_dups'] == 1] # Get the unique records
    fp = wd_path + "\\data\\raw\\twitter_openalex\\input_files\\fuzzy_twitter_dfs\\authors_tweeters_2024_02_duplicated_twitter_ids.csv" 
    t_df_dups.to_csv(fp, index=False)
    print("The file with TwitterId duplicates has been successfully saved.")
    
    # 4. Make a sub-division of the df into duplicates and non-duplicates (based on the OpenAlexId) and save the fuzzy data in a folder
    t_df_ndups['OpenAlexId_dups'] = t_df_ndups['OpenAlexId'].map(t_df_ndups['OpenAlexId'].value_counts()) # Count the duplicates
    t_df_ndups_oa_dups            = t_df_ndups[t_df_ndups['OpenAlexId_dups'] >  1] # Get the duplicates
    t_df_ndups_oa_ndups           = t_df_ndups[t_df_ndups['OpenAlexId_dups'] == 1] # Get the unique values
    fp = wd_path + "\\data\\raw\\twitter_openalex\\input_files\\fuzzy_twitter_dfs\\authors_tweeters_2024_02_duplicated_openalex_ids.csv" 
    t_df_ndups_oa_dups.to_csv(fp, index=False)
    print("The file with OpenAlexId duplicates has been successfully saved.")
   
    # 5. Save and return the clean df (1:1 between Twitter and OpenAlexIds)
    t_df_ndups_oa_ndups = t_df_ndups_oa_ndups.drop(columns=['TwitterId_dups', 'OpenAlexId_dups']) # Drop the columns with the duplicates counters
    t_df_ndups_oa_ndups.rename(columns={'OpenAlexId': 'PersonOpenAlexId'}, inplace=True)
    fp = wd_path + "\\data\\raw\\twitter_openalex\\input_files\\authors_tweeters_2024_02_clean.csv"
    t_df_ndups_oa_ndups.to_csv(fp, index=False)
    print("The clean 1:1 dataset has been successfully saved.")
    return t_df_ndups_oa_ndups

# Fn35: use_aux_df = Use the auxiliary file to know which authors to keep from the duplicates df
def merge_aux_df(main_df, aux_df):
    # 1. Prepare the auxiliary file
    sel_cols = ['PersonId','PersonOpenAlexId','keep']
    aux_df = aux_df[sel_cols] # Select the columns
    aux_df['PersonId'] = aux_df['PersonId'].astype(str) # Change the type of the column to string
    aux_df['key'] = aux_df['PersonId'] + aux_df['PersonOpenAlexId'] # Create a key to merge the two datasets
    aux_df = aux_df[['key','keep']] # Select the columns
    
    # 2. Prepare the main file
    main_df['PersonId'] = main_df['PersonId'].astype(str) # Change the type of the column to string
    main_df['key'] = main_df['PersonId'] + main_df['PersonOpenAlexId'] # Create a key to merge the two datasets

    # 3. Merge the two datasets
    non_dup_df = pd.merge(main_df, aux_df, on = 'key', how = 'left') # Merge the two datasets
    check_duplicates_and_missing_values(main_df,non_dup_df,"keep",False) # Check potential duplicates when merging the main and the auxiliary file
    # 3. Keep only unique authors and remove the 'key' column
    non_dup_df = non_dup_df[non_dup_df['keep'] == 'Yes'] # Keep only the authors to keep
    non_dup_df = non_dup_df.drop(columns=['key']) # Drop non useful columns
    return non_dup_df

# Fn36: gen_aarc_openalex_dictionary = Generate the AARC-OpenAlex dictionary depending on the type of dictionary (reduced vs full sample)
def gen_aarc_openalex_dictionary(wd_path, dictionary_type):
    # 4.0 Check that the user inputs the proper dictionary type
    valid_types = ["reduced sample", "full sample"]
    if dictionary_type not in valid_types:
        raise ValueError(f"Invalid dictionary_type: '{dictionary_type}'. Must be one of {valid_types}.")
    if dictionary_type == "reduced sample":
          # Input file path
          file_path       = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_dictionary.xlsx"
          # 4.1 Open the intermediate dictionary and isolate the duplicates and the missing values
          dict_df = pd.read_excel(file_path)
          missing_authors_df  = dict_df[dict_df['matched'] == 0] # Missing authors df
          nmissing_authors_df = dict_df[dict_df['matched'] == 1] # Matched authors df
          # Output file path
          final_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_dictionary.xlsx"
    if dictionary_type == "full sample":
          # Input file path 
          file_path_01 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_author_intermediate_businessecon_dictionary.xlsx"
          file_path_02 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_bernhard_procedure.xlsx"
          # 4.1 Open the intermediate dictionary, the bernhard procedure file, and isolate the duplicates and the missing values
          dict_df_01 = pd.read_excel(file_path_01)
          missing_authors_df_01  = dict_df_01[dict_df_01['matched'] == 0] # Missing authors df
          nmissing_authors_df_01 = dict_df_01[dict_df_01['matched'] == 1] # Matched authors df
          dict_df_02 = pd.read_excel(file_path_02)
          missing_authors_df_02  = dict_df_02[dict_df_02['matched'] == 0] # Missing authors df
          nmissing_authors_df_02 = dict_df_02[dict_df_02['matched'] == 1] # Matched authors df
          # 4.1.1 Concatenate the missing and nmissing dfs and then disconnect them
          missing_authors_df = pd.concat([missing_authors_df_01, missing_authors_df_02])
          nmissing_authors_df = pd.concat([nmissing_authors_df_01, nmissing_authors_df_02])         
          # 4.1.1.1 Define a temp_file to see the authors that were matched from the bernhard procedure
          temp_df = nmissing_authors_df.copy()
          temp_df = temp_df[['PersonId']]
          temp_df = temp_df.drop_duplicates()
          temp_df['dummy'] = 1
          # 4.1.1.2 Merge the missing authors and remove the ones that were matched in the bernhard procedure
          missing_authors_df = pd.merge(missing_authors_df, temp_df, on='PersonId', how='left')
          missing_authors_df = missing_authors_df[missing_authors_df['dummy'].isnull()]
          missing_authors_df = missing_authors_df.drop('dummy', axis=1)
          # Output file path
          final_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_businessecon_dictionary.xlsx"
    
    # 4.2 Solve the OpenAlexId duplicates problem
    # 4.2.1 Divide the df between unique and duplicates authors
    nmissing_authors_df['PersonId_dups'] = nmissing_authors_df['PersonId'].map(nmissing_authors_df['PersonId'].value_counts()) # Count the duplicates
    unique_authors_df = nmissing_authors_df[nmissing_authors_df['PersonId_dups'] == 1] # Get the unique authors
    duplicates_authors_df = nmissing_authors_df[nmissing_authors_df['PersonId_dups'] > 1] # Get the duplicates authors
    # 4.2.2. Use auxiliary file(s) to know which authors to keep from the duplicates df
    aux_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_handle_duplicates_manual_file.xlsx"
    aux_df = pd.read_excel(aux_fp,sheet_name="Sheet1") # Open the auxiliary file
    duplicates_authors_df = merge_aux_df(duplicates_authors_df, aux_df)
    
    # 4.4.3 For the Full sample handle new duplicates 
    if dictionary_type == "full sample":
        # 1. Open the second auxiliary file
        aux_fp_02 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_handle_duplicates_manual_file_02.xlsx"
        aux_df_02 = pd.read_excel(aux_fp_02,sheet_name="Sheet1") # Open the auxiliary file
        aux_df_02 = aux_df_02[aux_df_02['keep'].notnull()] # Work only with cases that have been checked
        # 2. Work only with the authors that have not been selected from the first auxiliary file
        solved_dups_df = duplicates_authors_df.copy()        # Copy the duplicates authors df
        solved_dups_df = solved_dups_df[['PersonId','keep']] # Select special columns
        solved_dups_df['PersonId'] = solved_dups_df['PersonId'].astype(int) # Put the PersonId back to an integer
        dups_authors_df = nmissing_authors_df[nmissing_authors_df['PersonId_dups'] > 1] # Get the duplicates authors
        working_df = pd.merge(dups_authors_df, solved_dups_df, on = 'PersonId', how = 'left') # Merge the two datasets
        working_df = working_df[working_df['keep'].isnull()] # Keep only the authors that have not been selected
        working_df = working_df.drop(columns=['keep'])       # Drop the 'keep' column to avoid errors
        # 3. Use the second auxiliary file to know which authors to keep from the duplicates df
        duplicates_authors_df_02 = merge_aux_df(working_df, aux_df_02)
        # 4. Concatenate the two datasets and remove duplicates (just in case)
        duplicates_authors_df = pd.concat([duplicates_authors_df, duplicates_authors_df_02], ignore_index=True)
        duplicates_authors_df = duplicates_authors_df.drop_duplicates() # Drop the duplicates
    
    # 4.2.4 Run a test to check if any duplicates remain 
    duplicates_authors_df['PersonId_dups_new'] = duplicates_authors_df['PersonId'].map(duplicates_authors_df['PersonId'].value_counts()) # Count the duplicates
    unique_values = duplicates_authors_df['PersonId_dups_new'].unique() # Get the unique values
    test_unique_values = unique_values.shape[0] # Get the number of unique values
    # CHANGE THIS WHEN I AM ABLE TO HANDLE THE DUPLICATES FROM THAT CAME FROM THE BERNHARD PROCEDURE
    #assert test_unique_values == 1, "Error: There are still duplicates in the dataset! Do a further check!"
    # 4.2.5 Concatenate the unique dataset with the corrected duplicates authors df and do a final check
    duplicates_authors_df = duplicates_authors_df.drop(columns=['keep','PersonId_dups_new']) # Drop non useful columns
    # 4.2.6 Concatenate the missing authors with the corrected duplicates authors
    nmissing_authors_new_df = pd.concat([unique_authors_df, duplicates_authors_df], ignore_index=True)
    nmissing_authors_new_df = nmissing_authors_new_df.drop(columns=['PersonId_dups']) # Drop non useful columns

    # 4.3 Add the manually searched authors to the final dictionary
    # 4.3.1 Open the manual authors file
    man_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\manual_aarc_openaalex_matches.xlsx"
    manual_authors_df = pd.read_excel(man_fp)
    manual_authors_df = manual_authors_df.drop(columns=['Comment','Resources']) # Drop non useful columns
    # 4.3.2 Do a final check to know that all the missing authors are in the manual authors df
    manual_aux_df = manual_authors_df[['PersonId']]
    manual_aux_df['dummy'] = 1  
    missing_authors_df = pd.merge(missing_authors_df, manual_aux_df, on = 'PersonId', how = 'left') # Merge the two datasets
    test_df = missing_authors_df[missing_authors_df['dummy'] != 1] # Get the missing authors
    test_manual_df = test_df.shape[0] # All of the missing authors are in the manual authors df! :)
    if dictionary_type == "reduced sample": # Run the test only for the reduced sample
        assert test_manual_df == 0, "Error: There are some missing authors that are not in the manual authors df! Do a further check!"
    # 4.3.3 Concatenate the missing authors with the new authors
    manual_authors_df['matched'] = False # Add a new column to the manual authors df to know that they were not originally matched
    final_authors_df = pd.concat([manual_authors_df, nmissing_authors_new_df], ignore_index=True)
    # 4.3.4 Rename the match column to improve readability
    final_authors_df.rename(columns = {'matched':'AutoMatch'}, inplace = True) # Rename the column to merge

    # 4.4 Save the final authors dictionary
    final_authors_df.to_excel(final_file_path, index = False)
    print(f"The {dictionary_type} dictionary was successfully saved!")
    print(f"The path to the file is: {final_file_path}")

    # 5. Return the final authors dictionary
    return final_authors_df

# Fn37: get_aarc_openalex_dictionary_progress = Get the AARC-OpenAlex dictionary progress
def get_aarc_openalex_dictionary_progress(wd_path):
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
    check_duplicates_and_missing_values(original_df = be_list_df, new_df = be_list_df_01,column_name ='PersonId',check_missing_values = False) # Check the duplicates and missing values
    # 7.6.2 Merge with those in the DOI files
    df_DOI_tm = df_DOI[['PersonId']] # Select the AARCId
    df_DOI_tm['DOI_dummy'] = 1       # Create a dummy variable to merge
    df_DOI_tm = df_DOI_tm.drop_duplicates() # Delete the duplicates
    be_list_df_02 = pd.merge(be_list_df_01,df_DOI_tm,how='left',on='PersonId') # Merge the BusinessEcon Faculty list with the DOI files
    check_duplicates_and_missing_values(original_df = be_list_df_01, new_df = be_list_df_02,column_name ='PersonId',check_missing_values = False) # Check the duplicates and missing values
    
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
    # 7.8 Save the file that contains the match status for every author
    be_list_df_02_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\BusinessEconFacultyLists_MatchingStatus.xlsx"
    be_list_df_02.to_excel(be_list_df_02_path, index=False) # Save the file to check the results
    print('BusinessEconFacultyLists_MatchingStatus.xlsx file saved, this file shows the matching status for every author')
    # 7.9 Provide a summary of the match status
    match_df = be_list_df_02.groupby('MatchStatus').size().reset_index(name='Counts') # Aggregate the data by 'MatchStatus' and count the occurrences
    total_c  = match_df['Counts'].sum() # Calculate the total of the 'Counts' column
    match_df['share'] = match_df['Counts'] / total_c # Add a new column 'share' that shows the share of each count with respect to the total
    print(match_df)

    # 7.9 Return the be_list_df_02 dataframe and the match_df dataframe
    return be_list_df_02, match_df

# Fn38: fix_name = Fix names by  capitalizing the first letter, and swapings the first and last names
def fix_name(name):
    name = name.title()            # Capitalize first letter of each word
    name = name.split(',')         # Split the name in two parts guided by the comma
    name = name[1] + ' ' + name[0] # Swap first and last names
    return name.strip()            # Remove leading/trailing spaces

# Fn39: remove_middle_name = Removes the middle names (keeps only first and last names)
def remove_middle_name(name):
    parts = name.split() # Split the name into parts (words) based on spaces
    return ' '.join([parts[0], parts[-1]]) if len(parts) > 2 else name

# Fn40: get_last_name = Gets the last name of a string
def get_last_name(name):
    return name.split()[-1]

# Fn41: bernhard_matching_procedure = Match the DOI-Author name using the Bernhard procedure
def bernhard_matching_procedure(wd_path):
    # 1. Open the inputs
    papers_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Bernhard_matching_help\\doi_papers_authors_openalex.csv"
    papers_df = pd.read_csv(papers_fp, encoding='latin-1')
    researchers_fp = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\Bernhard_matching_help\\aarc_doi_pending_matching.csv"
    researchers_df = pd.read_csv(researchers_fp)

    
    # 2. Follow Bernhard's matching procedure as it appears in the original main.py file
    # Dictionary to store matches per PersonId
    person_matches = {}

    # Process each row individually first
    for idx, researcher_row in enumerate(researchers_df.itertuples(index=False), start=1):
        if idx % 500 == 0:  # Print progress every n rows
            print(f"🔄 Processed {idx} rows...")
        #if idx == 20000:
            #break
        person_id = researcher_row.PersonId
        doi_to_match = researcher_row.doi
        formatted_name = fix_name(researcher_row.PersonName)

        # Find matching papers
        matched_papers = papers_df[papers_df['paper_doi'] == doi_to_match]

        found_author_ids = set()  # Track unique author_ids for this row

        for _, paper_row in matched_papers.iterrows():
            # Clean names
            raw_name_clean = str(paper_row['paper_raw_author_name']).replace('.', '').title().replace('-', ' ')
            display_name_clean = str(paper_row['author_display_name']).replace('.', '').title().replace('-', ' ')

            # Check for exact match
            if formatted_name in [raw_name_clean, display_name_clean]:
                found_author_ids.add(paper_row['author_id'])

        # If no match, try removing middle names
        if not found_author_ids:
            formatted_name = remove_middle_name(formatted_name)
            for _, paper_row in matched_papers.iterrows():
                raw_name_clean = remove_middle_name(str(paper_row['paper_raw_author_name']).replace('.', '').title())
                display_name_clean = remove_middle_name(str(paper_row['author_display_name']).replace('.', '').title())

                if formatted_name in [raw_name_clean, display_name_clean]:
                    found_author_ids.add(paper_row['author_id'])

        # If no matches after removing middle names, try matching by last name
        if not found_author_ids:
            last_name = get_last_name(formatted_name)
            last_name_matches = matched_papers[
                matched_papers['author_display_name'].str.contains(last_name, case=False, na=False)]

            if len(last_name_matches['author_display_name'].unique()) > 1:
                print(
                    f"⚠️ Multiple authors with the same last name ({last_name}) in DOI {doi_to_match}. Cannot match PersonId {person_id}.")
            else:
                # Proceed with usual matching by last name
                for _, paper_row in last_name_matches.iterrows():
                    raw_name_clean = str(paper_row['paper_raw_author_name']).replace('.', '').title().replace('-', ' ')
                    display_name_clean = str(paper_row['author_display_name']).replace('.', '').title().replace('-',
                                                                                                                ' ')

                    if last_name in [raw_name_clean.split()[-1], display_name_clean.split()[-1]]:
                        found_author_ids.add(paper_row['author_id'])

        # Store results for this PersonId
        if person_id not in person_matches:
            person_matches[person_id] = set()
        person_matches[person_id].update(found_author_ids)

    # Group results by PersonId and print relevant cases
    print("\n✅ Finished processing. Now summarizing results:\n")
    for person_id, author_ids in person_matches.items():
        if not author_ids:
            print(f"❌ No match found for PersonId {person_id}")
        elif len(author_ids) > 1:
            print(f"⚠️ Multiple matches for PersonId {person_id}: author_ids = {author_ids}")

    # Calculate the number of matched PersonIds (those with at least one author_id)
    matched_person_ids = sum(1 for author_ids in person_matches.values() if author_ids)

    # Calculate the total number of unique PersonIds in person_matches
    total_person_ids = len(person_matches)

    # Calculate and print the percentage of matched person_ids
    matched_percentage = (matched_person_ids / total_person_ids) * 100
    print(f"\n📊 {matched_percentage:.2f}% of PersonIds were matched.")


    # 3. Transform to a dataframe, save the file and return the dataframe
    # Transform the dictionary into a DataFrame to continue building the final dictionary
    person_matches_df = pd.DataFrame({
        "PersonId": list(person_matches.keys()),
        "PersonOpenAlexId": list(person_matches.values())
    })
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_berhnard_raw_file\\bernhard_procedure_author_match.csv"
    person_matches_df.to_csv(file_path, index=False, encoding='utf-8')
    print('The bernhard_procedure_author_match.csv file was correctly saved in the aarc_openalex_authors_matches_bernhard_raw_file folder')
    return person_matches_df

# Fn42: format_bernhard_matches = Format the bernhard matches to be used to create the final dictionary
def format_bernhard_matches(wd_path):
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
    check_duplicates_and_missing_values(author_matches_df, author_matches_dfm, 'PersonName', True)

    # 4. Add the 'PersonOpenAlexName' column from the AARC dataset
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
    ao_df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    # 4.1 Select columns, remove duplicates and rename columns
    ao_df = ao_df[['author_id', 'author_display_name']] # Select columns
    ao_df = ao_df.drop_duplicates() # Remove duplicates
    ao_df = ao_df.rename(columns={'author_id': 'PersonOpenAlexId', 'author_display_name': 'PersonOpenAlexName'}) # Rename columns
    # 4.2 Merge the two dataframes on 'PersonOpenAlexId' and do a quick check of the result
    author_matches_dfmm = pd.merge(author_matches_dfm, ao_df, on='PersonOpenAlexId', how='left')
    check_duplicates_and_missing_values(author_matches_dfm, author_matches_dfmm, 'PersonOpenAlexName', False) # It is put false beacause some values are null by definition

    # 5. Reorder the columns for aesthetic purposes
    author_matches_dfmm = author_matches_dfmm[['PersonId', 'PersonName', 'PersonOpenAlexId', 'PersonOpenAlexName']]
    
    # 6. Replace 'set()' for None for those unable to be matched and create a column that indicates if the author was matched or not
    author_matches_dfmm['matched'] = author_matches_dfmm['PersonOpenAlexName'].notnull()
    author_matches_dfmm['PersonOpenAlexId'] = author_matches_dfmm['PersonOpenAlexId'].replace("set()", None)
    
    # 7. Save the dataframes under the names 'aarc_openalex_authors_matches_bernhard_procedure.csv' and 'aarc_openalex_authors_nomatches_bernhard_procedure.csv'
    fp01 = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_bernhard_procedure.xlsx"
    author_matches_dfmm.to_excel(fp01, index=False)
    print("The file 'aarc_openalex_authors_matches_bernhard_procedure' was correctly saved")

    # 8. Return the dataframe
    return author_matches_dfmm

# End of file