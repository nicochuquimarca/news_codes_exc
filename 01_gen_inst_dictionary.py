                                # Research in the Media
# Objectives: 1. Produce a institution dictionary that connects institution ids (id and grid_id_string) 
#             2. With the dictionary in hand, filter authors coming from institutions of interest
#             3. Filter those authors that have their second or third affiliation in the US and not in the previous dataset 
#             4. Get a number of works produced by year for each author in the final dataset 
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-01-21: First version
# version 1.1: 2025-01-24: Search for the second and third author affiliations and check if they belong to the US and not in the previous dataset
# version 1.2: 2025-01-25: Generate the final dataset with the authors that have their second or third affiliation in the US
# version 2.1: 2025-01-28: Connect to the API to search for the years in which each author produced a publication
# version 2.2: 2025-01-29: Continue with the API connection and save the results in a DataFrame, do testing and save the results
# version 3.1: 2025-02-01: Produce Institution authors output specific datasets and save them
# version 4.1: 2025-04-02: Generate Caltech researchers dataset and save it
# version 4.2: 2025-06-02: Continue with the Caltech researchers dataset and save it

# Function List
# Fn01: num_affs_tests = Get the max number of affiliations per author and check for potential NULL values
# Fn02: open_and_do_minimal_cleaning = Get the number of affiliations per author (observation level)
# Fn03: extract_affiliation_details = Function to extract affiliation details efficiently
# Fn04: extract_and_filter_affs_details = Convert the 'affiliations' column from JSON strings to Python objects
# Fn05: group_affs_ids = Group the affiliation ids and remove duplicates
# Fn06: get_one_institution_info = Get the information of one institution
# Fn07: date_time_string = Get the current date and time in a string format
# Fn08: get_institution_info = Get the information of multiple institutions
# Fn09: gen_ids_to_call = Generate the ids to call the API
# Fn10: gen_final_institutions_info_csv = Append all the CSV files in a folder
# Fn11: generate_scrap_batches = Divide the Dataframe into the scrap batches
# Fn12: extract_mult_affiliation_details = Function to extract multiple affiliation details efficiently (extract the second and third affiliation)
# Fn13: extract_and_filter_affs_details_mult_affs = Convert the 'affiliations' column from JSON strings to Python objects for more than one affiliation
# Fn14: filter_affs_2and3_US = Filter the authors with their second or third affiliation in the US
# Fn15: prepare_dict_for_temp_merge = Prepare the dictionary for the temporary merge
# Fn16: fill_na_ycols_to_the_left = Fill the missing years to the left when creating a DataFrame
# Fn17: fill_na_ycols_to_the_right = Fill the missing years to the right when creating a DataFrame
# Fn18: handle_missing_years = Handle the missing years in the years vector
# Fn19: format_author_df = Format the final df the get_works_by_year function produces
# Fn20: gen_empty_author_df = Generate an empty DataFrame for authors that have no data (either not found by the API or deleted)
# Fn21: get_works_by_year = Get the works by year for a given author_id
# Fn22: process_author_id = Function that enable multiple workers to call the get_works_by_year function
# Fn23: linear_works_by_year = Linear process to get the works by year for a list of authors
# Fn24: parallel_works_by_year = Parallel process to get the works by year for a list of authors
# Fn25: gen_final_works_by_year_csv = Generate the final works by year DataFrame
# Fn26: get_uni_ror = Get the ror code for a given university name
# Fn27: gen_authors_ids_to_call = Generate the authors ids to call the API
# Fn28: format_df_author_works = Format the intermediate DataFrame for the author works by year
# Fn29: gen_author_works_by_institution = Generate the authors works by year for a given institution
# Fn30: caltech_affs_matched_row = Intermediate function to see which of the affiliation matched with caltech
# Fn31: format_caltech_raw_authors_dfs = Format the raw authors DataFrames for Caltech

# Pseudo Code
# 1. Working directory
# 2. Open and do minimal cleaning
# 3. Get the institutions ids, collapse them and then remove duplicates
# 4. Prepare the information of the institutions that will call the API
# 5. Append all the CSV files in a folder to be able to deliver a matching ids for institutions dataset
# 6. Open the Dictionary and the big DataFrame (that contains the institution ids)
# 7. Do a left-join to get the final DataFrame (The final dictionary)
# 8. Start with the author filtering
# 9. Merge the final dictionary with the authors DataFrame and filter the authors in institutions of interest
# 10. Generate a dataset for people whos second or third affiliation country code is the US but their first affiliation is not in the previous dataset
# 11. Merge the new dataset with the final dictionary in an iterative way to not overload the memory
# 12. Look for potential duplicates from the previous dataset, drop them and save the final dataset
# 13. Generate a dataset with the number of works by year (from 2012 to 2025) for each author by calling the API
# 14. Produce institution specific works by year DataFrame (replaced flow code with a fn)
# 15. Generate the datasets for people whos first affiliation is Caltech
# 16. Generate the datasets for people whos second or third affiliation is Caltech
# 17. Look for potential duplicates from the previous dataset first affiliation dataset, drop them and save the final dataset
# 18. Produce the Caltech specific works by year DataFrame (the parameters in this case must remain fixed)

# 0. Packages
import os, pandas as pd, ast, requests, math
from datetime import date                      # Get the current date
from datetime import datetime                  # Get the current date and time
import concurrent.futures                      # For parallel processing
import time                                    # For time measurement

# Fn01: num_affs_tests = Get the max number of affiliations per author and check for potential NULL values
def num_affs_tests(df):
    # Get the maximum number of affiliations per author
    uaffs_count = df['affs_count'].unique()
    max_affs = uaffs_count.max()
    print(f"Maximum number of affiliations in this df: {max_affs}")

    # Check for NULL values in the affs_count column
    missing_values = df['affs_count'].isnull().sum()
    assert missing_values == 0, "There are missing values in the column afss_count, please check \n"
    
    # Return the max number of affiliations
    return max_affs

# Fn02: open_and_do_minimal_cleaning = Get the number of affiliations per author (observation level)
def open_and_do_minimal_cleaning(file_path, filter, filter_value): 
    # Open the DataFrame
    df = pd.read_csv(file_path)
    # Get the number of affiliations per author (observation) = count the num of key { in the obs
    df['affs_count'] = df['affiliations'].str.count('}')/2 
    # Get the max number of affiliations and check for NULL values
    max_num_affs = num_affs_tests(df)
    if filter == True:
        # Filter the authors with only one affiliation
        df = df[df['affs_count'] == filter_value]
    else: 
        None
    return max_num_affs, df

# Fn03: extract_affiliation_details = Function to extract affiliation details efficiently
def extract_affiliation_details(affiliations):
    if affiliations:
        affiliation = affiliations[0]  # Assuming there's at least one affiliation
        return affiliation['institution']['id'], affiliation['institution']['country_code'], affiliation['institution']['display_name']
    return None, None, None

# Fn04: extract_and_filter_affs_details = Convert the 'affiliations' column from JSON strings to Python objects
def extract_and_filter_affs_details(df):
    df['affiliations'] = df['affiliations'].apply(ast.literal_eval)
    # Extract the details using vectorized operations
    df['affiliation_id'], df['country_code'], df['institution_display_name'] = zip(*df['affiliations'].apply(extract_affiliation_details))
    # Filter for only those that are in the 'US'
    df_us = df[df['country_code'] == 'US']
    return df_us

# Fn05: group_affs_ids = Group the affiliation ids and remove duplicates 
def group_affs_ids(df, version):
    # Select affiliation_id and institution_display_name
    if version == "single affiliation": # The original version
        sel_columns = ['affiliation_id', 'institution_display_name']
        df_new = df[sel_columns]
    # Select affiliation_id and institution_display_name for the multiple case scenario
    if version == "multiple affiliations": # The second version
        sel_columns_1 = ['affiliation_id1', 'institution_display_name1']
        df_temp_1 = df[sel_columns_1]
        df_temp_1.rename(columns = {'affiliation_id1':'affiliation_id','institution_display_name1':'institution_display_name'}, inplace = True)
        sel_columns_2 = ['affiliation_id2', 'institution_display_name2']
        df_temp_2 = df[sel_columns_2]
        df_temp_2.rename(columns = {'affiliation_id2':'affiliation_id','institution_display_name2':'institution_display_name'}, inplace = True)
        sel_columns_3 = ['affiliation_id3', 'institution_display_name3']
        df_temp_3 = df[sel_columns_3]
        df_temp_3.rename(columns = {'affiliation_id3':'affiliation_id','institution_display_name3':'institution_display_name'}, inplace = True)
        df_new = pd.concat([df_temp_1,df_temp_2,df_temp_3], axis = 0)
    
    # Work with the simple two column dataframe
    df_new = df_new.dropna(subset=['affiliation_id'])
    df_new = df_new.drop_duplicates(subset=['affiliation_id'])
    df_new['affiliation_id_red'] = df_new['affiliation_id'].apply(lambda x: x.split('/')[-1])
    df_new['aff_id_API'] = "https://api.openalex.org/institutions/" + df_new['affiliation_id_red']
    # Return the DataFrame 
    return df_new

# Fn06: get_one_institution_info = Get the information of one institution
def get_one_institution_info(id):
    # Call the API
    id_response = requests.get(id)
    response_test = id_response.status_code
    # If there is no info return an empty dataframe
    if response_test != 200:
        # Define and return an empy dataframe
       cnames = ['id', 'display_name', 'country_code', 'openalex', 'ror', 'mag', 'grid', 'wikipedia', 'wikidata']
       df_id = pd.DataFrame(columns=cnames)
    # If there is info return the dataframe
    elif response_test == 200:
        id_data  = id_response.json()
        ids_info = id_data.get('ids', {})
        # Get individual information
        id_info  = id_data.get('id') 
        id_display_name = id_data.get('display_name')
        id_country_code = id_data.get('country_code')
        ids_info_openalex = ids_info.get('openalex')
        ids_info_ror = ids_info.get('ror')
        ids_info_mag = ids_info.get('mag')
        ids_info_grid = ids_info.get('grid')
        ids_info_wikipedia = ids_info.get('wikipedia')
        ids_info_wikidata = ids_info.get('wikidata')
        # Create a DataFrame with the individual information
        df_id = pd.DataFrame([{
            'id': id_info,
            'display_name': id_display_name,
            'country_code': id_country_code,
            'openalex': ids_info_openalex,
            'ror': ids_info_ror,
            'mag': ids_info_mag,
            'grid': ids_info_grid,
            'wikipedia': ids_info_wikipedia,
            'wikidata': ids_info_wikidata
        }])
    
    # Return the DataFrame
    return df_id

# Fn07: date_time_string = Get the current date and time in a string format
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

# Fn08: get_institution_info = Get the information of multiple institutions
def get_institution_info(wd_path,ids_vector):
    # Get the total number of ids to call the API
    total_num_ids = str(len(ids_vector))
    # Initialize an empty lists to store the results
    institutions_list = []
    # Iterate over the ids
    for current_iter, id in enumerate(ids_vector, start=1):  # start=1 to start counting from 1
        # Get the df of the current id
        df_id = get_one_institution_info(id)
        # Append the df to the list
        institutions_list.append(df_id)
    
    # Concatenate the list of dataframes
    institutions_df = pd.concat(institutions_list, axis = 0)
    # Save the DataFrame
    # Get the dates to save the files
    current_time = datetime.now()
    date_string  = date_time_string(current_time = current_time)
    path = wd_path + "\\data\\raw\\open_alex\\institutions_files\\inst_info_"+date_string+".csv"
    institutions_df.to_csv(path, index = False)
    print("Institutions information has been saved in the following path: ", path)
    # Return the DataFrame
    return institutions_df

# Fn09: gen_ids_to_call = Generate the ids to call the API
def gen_ids_to_call(wd_path):
    # Open the data
    df_path = wd_path + "\\data\\raw\\open_alex\\openalex_institutions_ids_02.csv"
    df = pd.read_csv(df_path)
    df = df[['affiliation_id','aff_id_API']]
    df.rename(columns = {'affiliation_id':'id'}, inplace = True)
    
    
    # Open the final_df to check if the id is already in the final_df
    fdf_path = wd_path + "\\data\\raw\\open_alex\\openalex_institutions_final_info.csv"
    fdf = pd.read_csv(fdf_path)
    fdf = fdf[['id']]
    fdf['dummy_col'] = 1
    
    # Merge the two DataFrames to get which ones have not been called
    merged_df = pd.merge(df, fdf, on = "id", how = "left")

    # Get rid of those observations with dummy_col = 1 in the merged_df
    merged_df = merged_df[merged_df['dummy_col'].isnull()]

    # Return only the ids to call
    merged_df = merged_df[['aff_id_API']]

    # Return the pending to scrap DataFrame
    return merged_df

# Fn10: gen_final_institutions_info_csv = Append all the CSV files in a folder
def gen_final_institutions_info_csv(wd_path):
    folder_path = wd_path + "\\data\\raw\\open_alex\\institutions_files"
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
    # 5. Save the final DataFrame
    final_file_path = wd_path + "\\data\\raw\\open_alex\\openalex_institutions_final_info.csv"
    final_df.to_csv(final_file_path, index = False)
    print("The final institutions information has been saved successfully")
    return final_df

# Fn11: generate_scrap_batches = Divide the Dataframe into the scrap batches
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

# Fn12: extract_mult_affiliation_details = Function to extract multiple affiliation details efficiently (extract the second and third affiliation)
def extract_mult_affiliation_details(affiliations):
    if affiliations and len(affiliations) == 1:
        affiliation1 = affiliations[0]  # Assuming there's is only one affiliation
        return (affiliation1['institution']['id'], affiliation1['institution']['country_code'], affiliation1['institution']['display_name'],
                None,None,None,
                None,None, None)
    if affiliations and len(affiliations) == 2:
        affiliation1 = affiliations[0]
        affiliation2 = affiliations[1]
        return (affiliation1['institution']['id'], affiliation1['institution']['country_code'], affiliation1['institution']['display_name'],
                affiliation2['institution']['id'], affiliation2['institution']['country_code'], affiliation2['institution']['display_name'],
                None,None, None)
    if affiliations and len(affiliations) >= 3:
        affiliation1 = affiliations[0]
        affiliation2 = affiliations[1]
        affiliation3 = affiliations[2]
        return (affiliation1['institution']['id'], affiliation1['institution']['country_code'], affiliation1['institution']['display_name'],
                affiliation2['institution']['id'], affiliation2['institution']['country_code'], affiliation2['institution']['display_name'],
                affiliation3['institution']['id'], affiliation3['institution']['country_code'], affiliation3['institution']['display_name'])
    return (None, None, None, 
            None, None, None,
            None, None, None)

# Fn13: extract_and_filter_affs_details_mult_affs = Convert the 'affiliations' column from JSON strings to Python objects for more than one affiliation
def extract_and_filter_affs_details_mult_affs(df):
    # Filter to get only those with more than one affiliation (due to computational constraints and because the first affiliation was already used to generate the first dataset)
    df = df[df['affs_count'] > 1]
    # Convert the 'affiliations' column from JSON strings to Python objects
    df['affiliations'] = df['affiliations'].apply(ast.literal_eval)
    # Extract the details using vectorized operations
    (df['affiliation_id1'], df['country_code1'], df['institution_display_name1'],
     df['affiliation_id2'], df['country_code2'], df['institution_display_name2'],
     df['affiliation_id3'], df['country_code3'], df['institution_display_name3']) = zip(*df['affiliations'].apply(extract_mult_affiliation_details))
    return df

# Fn14: filter_affs_2and3_US = Filter the authors with their second or third affiliation in the US
def filter_affs_2and3_US(df):
    df = df[(df['country_code2'] == 'US') | (df['country_code3'] == 'US')]
    return df

# Fn15: prepare_dict_for_temp_merge = Prepare the dictionary for the temporary merge
def prepare_dict_for_temp_merge(df,affs_num):
    df = df[['id']]
    if affs_num == 1:
        df.rename(columns = {'id':'id1'}, inplace = True)
        df['dummy_col_aff1'] = 1
    if affs_num == 2:
        df.rename(columns = {'id':'id2'}, inplace = True)
        df['dummy_col_aff2'] = 1
    if affs_num >= 3:
        df.rename(columns = {'id':'id3'}, inplace = True)
        df['dummy_col_aff3'] = 1
    return df

# Fn16: fill_na_ycols_to_the_left = Fill the missing years to the left when creating a DataFrame
def fill_na_ycols_to_the_left(df,max_year):
    num_lfill_columns = 2025 - max_year
    lfill_column_names = ['y' + str(max_year + 1 + i) for i in range(num_lfill_columns)]
    # Add the new columns with None values
    for col in lfill_column_names:
        df[col] = None
    # Return the DataFrame
    return df

# Fn17: fill_na_ycols_to_the_right = Fill the missing years to the right when creating a DataFrame
def fill_na_ycols_to_the_right(df,min_year):
    num_rfill_columns = min_year - 2012
    rfill_column_names = ['y' + str(min_year - 1 - i) for i in range(num_rfill_columns)]
    # Add the new columns with None values
    for col in rfill_column_names:
        df[col] = None
    # Return the DataFrame
    return df

# Fn18: handle_missing_years = Handle the missing years in the years vector
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

# Fn19: format_author_df = Format the final df the get_works_by_year function produces
def format_author_df(df,aid_id,aid_display_name,author_id):
    # Add some extra columns to the dataframe
    df['author_id'] = aid_id
    df['author_display_name'] = aid_display_name
    df['author_id_API'] = author_id
    # Reorder the DataFrame in a desired way
    desired_order = ['author_id', 'author_display_name', 'author_id_API'] # Put this columns into the first positions
    remaining_columns = [col for col in df.columns if col not in desired_order] # Get the remaining columns
    new_order = desired_order + remaining_columns # Combine the desired order with the remaining columns
    df = df[new_order] # Reorder the DataFrame
    return df

# Fn20: gen_empty_author_df = Generate an empty DataFrame for authors that have no data (either not found by the API or deleted)
def gen_empty_author_df(aid_id,aid_display_name,author_id):
    # Define and return an empty dataframe
    df = pd.DataFrame({
        'y2025': [None],'y2024': [None],'y2023': [None],'y2022': [None],
        'y2021': [None],'y2020': [None],'y2019': [None],'y2018': [None],
        'y2017': [None],'y2016': [None],'y2015': [None],'y2014': [None],
        'y2013': [None],'y2012': [None]
    })
    df = format_author_df(df,aid_id,aid_display_name,author_id)
    return df

# Fn21: get_works_by_year = Get the works by year for a given author_id
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
        df = gen_empty_author_df(aid_id,aid_display_name,author_id)
        return df
    elif aid_response_test == 200:
        # Get the data
        aid_data  = aid_response.json()
        aid_counts_b_year = aid_data.get('counts_by_year',[])
        aid_display_name  = aid_data.get('display_name')
        aid_id = aid_data.get('id')
        if aid_display_name == 'Deleted Author':
            # If the response is not successful return an empty DataFrame
            aid_display_name = 'Deleted Author'
            extract_id = lambda author_id: author_id.split('/')[-1]
            idnum = extract_id(author_id) # Use the lambda function
            aid_id = 'https://openalex.org/' + idnum
            df = gen_empty_author_df(aid_id,aid_display_name,author_id)
            return df
        if not aid_counts_b_year:
            # If the aid_counts_b_year is empty return an empty DataFrame
            extract_id = lambda author_id: author_id.split('/')[-1]
            idnum = extract_id(author_id) # Use the lambda function
            aid_id = 'https://openalex.org/' + idnum
            df = gen_empty_author_df(aid_id,aid_display_name,author_id)
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
            df = format_author_df(df,aid_id,aid_display_name,author_id)
            return df

# Fn22: process_author_id = Function that enable multiple workers to call the get_works_by_year function
def process_author_id(author_id):
    # 3 workers --> time sleep = 0.05
    # 4 workers --> time sleep = 0.20
    time.sleep(0.06) # Introduce a small delay between requests
    return get_works_by_year(author_id)

# Fn23: linear_works_by_year = Linear process to get the works by year for a list of authors
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
    path = wd_path + "\\data\\raw\\open_alex\\authors_works_by_year\\authors_works_by_y_"+date_string+".csv"
    linear_authors_df.to_csv(path, index = False)
    print("Institutions information has been saved in the following path: ", path)
    # Return the DataFrame
    return linear_authors_df

# Fn24: parallel_works_by_year = Parallel process to get the works by year for a list of authors
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
    path = wd_path + "\\data\\raw\\open_alex\\authors_works_by_year\\authors_works_by_y_"+date_string+".csv"
    parallel_authors_df.to_csv(path, index = False)
    print("Institutions information has been saved in the following path: ", path)
    # Return the DataFrame
    return parallel_authors_df

# Fn25: gen_final_works_by_year_csv = Generate the final works by year DataFrame
def gen_final_works_by_year_csv(wd_path):
    folder_path = wd_path + "\\data\\raw\\open_alex\\authors_works_by_year"
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
    # 5. Save the final DataFrame
    final_file_path = wd_path + "\\data\\raw\\open_alex\\openalex_authors_works_by_years.csv"
    final_df.to_csv(final_file_path, index = False)
    print("The final institutions information has been saved successfully")
    return final_df

# Fn26: get_uni_ror = Get the ror code for a given university name
def get_uni_ror(uni_name):
    # Open the file
    name_ror_fpath = wd_path + "\\data\\raw\\open_alex\\institutions_name_ror.xlsx"
    name_ror_df    = pd.read_excel(name_ror_fpath)
    # Filter the file
    name_ror_df    = name_ror_df[name_ror_df['aarc_name'] == uni_name] 
    # Run a test whether the filtering is working
    rows = name_ror_df.shape[0]
    assert rows == 1, f"{uni_name} not found, please check the name"
    print(f"{uni_name} found")
    # Get the ror code and return to the user
    ror_code = name_ror_df['ror'].iloc[0]
    return ror_code

# Fn27: gen_authors_ids_to_call = Generate the authors ids to call the API
def gen_authors_ids_to_call(wd_path, uni_name, source):
    # Open the data
    # Lets prepare the data to call the get_works_by_year function multiple times
    if source == "main":
        file_path = "data\\raw\\open_alex\\openalex_authors_final.csv"
    elif source == "secondary":
        file_path = "data\\raw\\open_alex\\openalex_authors_final_affs2and3.csv"
    elif source == "main_caltech":
        file_path = "data\\raw\\open_alex\\openalex_caltech_authors_final.csv"
    elif source == "secondary_caltech":
        file_path = "data\\raw\\open_alex\\openalex_caletch_authors_final_affs2and3.csv"
    df_authors = pd.read_csv(file_path)
    # Filter the University using the ror code
    ror_code = get_uni_ror(uni_name = uni_name)
    df_authors = df_authors[df_authors['ror'] == ror_code]
    if source == "main" or source == "secondary":
            df_authors.rename(columns = {'pub_id':'author_id'}, inplace = True)
    elif source == "main_caltech" or source == "secondary_caltech":
            df_authors.rename(columns = {'PersonId':'author_id'}, inplace = True)
    df_authors['author_id_red'] = df_authors['author_id'].apply(lambda x: x.split('/')[-1])
    df_authors['author_id_API'] = "https://api.openalex.org/people/" + df_authors['author_id_red']
    # Select specific columns
    sel_cols = ['author_id_API']
    df_authors = df_authors[sel_cols]

    # Open the final_df to check if the id is already in the final_df
    fdf_path = wd_path + "\\data\\raw\\open_alex\\openalex_authors_works_by_years.csv"
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

# Fn28: format_df_author_works = Format the intermediate DataFrame for the author works by year
def format_df_author_works(df):
    # 1. Replace the NaN values with 0
    df_authors_works = df.fillna(0)
    # 2. Sum the columns of works by each individual year
    columns_to_sum = ['y2025', 'y2024', 'y2023', 'y2022', 'y2021', 'y2020', 'y2019', 'y2018', 'y2017', 'y2016', 'y2015', 'y2014', 'y2013', 'y2012']
    df_authors_works['works_2012_2025'] = df_authors_works[columns_to_sum].sum(axis=1)
    # 3. Get the ID of the author without the URL component
    df_authors_works['PersonId'] = df_authors_works['author_id_API'].apply(lambda x: x.split('/')[-1])
    # 4. Rename column for the final df
    df_authors_works.rename(columns = {'author_display_name':'author_API_name'}, inplace = True)
    # 5. Select the columns to keep
    sel_columns = ['PersonId','works_2012_2025','y2025','y2024','y2023','y2022','y2021','y2020','y2019','y2018','y2017','y2016','y2015','y2014','y2013','y2012','author_API_name']
    df_authors_works = df_authors_works[sel_columns]
    # 6. Drop duplicates
    df_authors_works = df_authors_works.drop_duplicates()
    # 7. Return the DataFrame
    return df_authors_works

# Fn29: gen_author_works_by_institution = Generate the authors works by year for a given institution
def gen_author_works_by_institution(wd_path,uni_name,file_name,data_source):
    # 1. Open the data source Berhard of Caltech Manual 
    if data_source == "Bernhard":
        file_path = wd_path + "\\data\\raw\\open_alex\\openalex_cleaned_names_all_affs.csv"
        df_authors_new = pd.read_csv(file_path) # Data provided by Bernhard
    elif data_source == "Caltech":
        file_path_caltech1 = wd_path + "\\data\\raw\\open_alex\\openalex_caltech_authors_final.csv"
        file_path_caltech2 = wd_path + "\\data\\raw\\open_alex\\openalex_caletch_authors_final_affs2and3.csv"
        caltech1 = pd.read_csv(file_path_caltech1)
        caltech2 = pd.read_csv(file_path_caltech2)
        df_authors_new = format_caltech_raw_authors_dfs(caltech1,caltech2)
    # 2. Open the university dictionary
    file_path = wd_path + "\\data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered_with_id.csv"
    df_dictionary = pd.read_csv(file_path) # University dictionary
    sel_columns = ['ipeds_id','ipeds_name','ror'] # Filter the columns
    df_dictionary = df_dictionary[sel_columns] # Filter the columns
    # 3. Merge the two DataFrames
    df_authors_ror = pd.merge(df_authors_new, df_dictionary, on = "ipeds_id", how = "left")
    # 4. Filter only the university specific authors
    ror_code = get_uni_ror(uni_name = uni_name)
    df_authors_uni = df_authors_ror[df_authors_ror['ror'] == ror_code]
    # 5. Open the authors works by year DataFrame
    file_path = wd_path + "\\data\\raw\\open_alex\\openalex_authors_works_by_years.csv"
    df_authors_works = pd.read_csv(file_path)
    df_authors_works = format_df_author_works(df_authors_works)
    # 6. Merge with the University specific authors
    df_authors_uni_works = pd.merge(df_authors_uni, df_authors_works, on = "PersonId", how = "left")
    # 7. Test if there are still authors to query
    null_df = df_authors_uni_works[df_authors_uni_works['works_2012_2025'].isnull()]
    rtest = null_df.shape[0]
    if rtest == 0:
        print("All authors have been queried")
    else:
        print("There are still authors to query, either run the secondary source or the get_missing_authors fn")
    # 8. Save the file and print a message to the user and return the DataFrame
    df_authors_uni_works.to_csv("data\\raw\\open_alex\\openalex_"+file_name+"_authors_works_by_years.csv", index = False)
    print("The DataFrame has been saved successfully")
    return df_authors_uni_works

# Fn30: caltech_affs_matched_row = Intermediate function to see which of the affiliation matched with caltech
def caltech_affs_matched_row(row):
    if row['affiliation_used_for_merge'] == 2:
        return row['institution_display_name2']
    elif row['affiliation_used_for_merge'] == 3:
        return row['institution_display_name3']
    else:
        return row['institution_display_name1']

# Fn31: format_caltech_raw_authors_dfs = Format the raw authors DataFrames for Caltech
def format_caltech_raw_authors_dfs(caltech1,caltech2):
    # 1. Format caltech1
    sel_cols1 = ['PersonId','author_display_name','institution_display_name','works_count','cited_by_count','aarc_id','ipeds_id'] # Set the columns to keep
    caltech1 = caltech1[sel_cols1] # Filter the columns
    caltech1.rename(columns = {'institution_display_name':'institution_name'}, inplace = True) # Renmae the affiliation name column
    # 2. Format caltech2
    sel_cols2 = ['PersonId','author_display_name','institution_display_name1','institution_display_name2',
             'institution_display_name3','affiliation_used_for_merge','works_count','cited_by_count','aarc_id','ipeds_id']
    caltech2 = caltech2[sel_cols2]
    caltech2['institution_name'] = caltech2.apply(caltech_affs_matched_row, axis=1)
    sel_cols2_new = ['PersonId','author_display_name','institution_name','works_count','cited_by_count','aarc_id','ipeds_id']
    caltech2 = caltech2[sel_cols2_new]
    # 3. Concatenate the two DataFrames
    caltech = pd.concat([caltech1,caltech2], axis = 0)
    caltech.rename(columns = {'PersonId':'PersonId_wf'}, inplace = True) # Renmae to create a new one
    caltech['PersonId'] = caltech['PersonId_wf'].apply(lambda x: x.split('/')[-1])  # Extract the ID
    # 4. Delete the temp col from caltech DataFrame
    caltech.drop(columns=['PersonId_wf'], inplace=True)
    # 5. Set PersonId as the first column
    caltech = caltech[sel_cols2_new]
    # 6. Return the DataFrame
    return caltech


# 1. Working directory
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)
pd.set_option('display.max_columns', None)

# 2. Open and do minimal cleaning
# File paths
file_path_01 = "data\\raw\\open_alex\\openalex_authors_1.csv"
file_path_02 = "data\\raw\\open_alex\\openalex_authors_2.csv"
file_path_03 = "data\\raw\\open_alex\\openalex_authors_3.csv"
file_path_04 = "data\\raw\\open_alex\\openalex_authors_4.csv"
file_path_05 = "data\\raw\\open_alex\\openalex_authors_5.csv"
file_path_06 = "data\\raw\\open_alex\\openalex_authors_6.csv"
# Get the maximum number of affiliations per author and the DataFrame with minimal cleaning
max_naffs_01, df_01 = open_and_do_minimal_cleaning(file_path_01, filter = True, filter_value = 1)
df_01_us = extract_and_filter_affs_details(df_01)
max_naffs_02, df_02 = open_and_do_minimal_cleaning(file_path_02, filter = True, filter_value = 1)
df_02_us = extract_and_filter_affs_details(df_02)
max_naffs_03, df_03 = open_and_do_minimal_cleaning(file_path_03, filter = True, filter_value = 1)
df_03_us = extract_and_filter_affs_details(df_03)
max_naffs_04, df_04 = open_and_do_minimal_cleaning(file_path_04, filter = True, filter_value = 1)
df_04_us = extract_and_filter_affs_details(df_04)
max_naffs_05, df_05 = open_and_do_minimal_cleaning(file_path_05, filter = True, filter_value = 1)
df_05_us = extract_and_filter_affs_details(df_05)
max_naffs_06, df_06 = open_and_do_minimal_cleaning(file_path_06, filter = True, filter_value = 1)
df_06_us = extract_and_filter_affs_details(df_06)

# 3. Get the institutions ids, collapse them and then remove duplicates
ids_01 = group_affs_ids(df = df_01_us,version = "single affiliation")
ids_02 = group_affs_ids(df = df_02_us,version = "single affiliation")
ids_03 = group_affs_ids(df = df_03_us,version = "single affiliation")
ids_04 = group_affs_ids(df = df_04_us,version = "single affiliation")
ids_05 = group_affs_ids(df = df_05_us,version = "single affiliation")
ids_06 = group_affs_ids(df = df_06_us,version = "single affiliation")
combined_ids_df = pd.concat([ids_01,ids_02,ids_03,ids_04,ids_05, ids_06], axis = 0)
combined_ids_df.drop_duplicates(subset='affiliation_id_red', keep='first', inplace=True)
#combined_ids_df.to_excel("data\\raw\\open_alex\\openalex_institutions_ids.xlsx", index = False)
#combined_ids_df.to_csv("data\\raw\\open_alex\\openalex_institutions_ids.csv", index = False)

# 4. Prepare the information of the institutions that will call the API
ids_to_call_df = gen_ids_to_call(wd_path=wd_path)
ids_batch = generate_id_batches(df=ids_to_call_df,batch_size=100)
num_batches  = 100
for i in range(0,num_batches):
        print("Current batch: ", i)
        id_vec = ids_batch[i]
        fdf = get_institution_info(wd_path = wd_path, ids_vector = id_vec)

# 5. Append all the CSV files in a folder to be able to deliver a matching ids for institutions dataset 
x = gen_final_institutions_info_csv(wd_path = wd_path)

# 6. Open the Dictionary and the big DataFrame (that contains the institution ids)
dictionary_path = wd_path + "\\data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered.xlsx"
dictionary_df   =  pd.read_excel(dictionary_path)
big_df_path = wd_path + "\\data\\raw\\open_alex\\openalex_institutions_final_info.csv"
big_df = pd.read_csv(big_df_path)
big_df.rename(columns = {'grid':'grid_id_string'}, inplace = True)

# 7. Do a left-join to get the final DataFrame (The final dictionary)
final_dictionary = pd.merge(dictionary_df, big_df, on = "grid_id_string", how = "left")
#final_dictionary.to_csv("data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered_with_id.csv", index = False)

# 8. Start with the author filtering
max_naffs_01, df_01 = open_and_do_minimal_cleaning(file_path_01, filter = False, filter_value = 1)
df_01_us = extract_and_filter_affs_details(df_01)
max_naffs_02, df_02 = open_and_do_minimal_cleaning(file_path_02, filter = False, filter_value = 1)
df_02_us = extract_and_filter_affs_details(df_02)
max_naffs_03, df_03 = open_and_do_minimal_cleaning(file_path_03, filter = False, filter_value = 1)
df_03_us = extract_and_filter_affs_details(df_03)
max_naffs_04, df_04 = open_and_do_minimal_cleaning(file_path_04, filter = False, filter_value = 1)
df_04_us = extract_and_filter_affs_details(df_04)
max_naffs_05, df_05 = open_and_do_minimal_cleaning(file_path_05, filter = False, filter_value = 1)
df_05_us = extract_and_filter_affs_details(df_05)
max_naffs_06, df_06 = open_and_do_minimal_cleaning(file_path_06, filter = False, filter_value = 1)
df_06_us = extract_and_filter_affs_details(df_06)
# Concatenate the DataFrames
df_us = pd.concat([df_01_us,df_02_us,df_03_us,df_04_us,df_05_us,df_06_us], axis = 0)
#df_us.to_csv("data\\raw\\open_alex\\openalex_authors_ready_to_match_dict.csv", index = False)

# 9. Merge the final dictionary with the authors DataFrame and filter the authors in institutions of interest
final_dictionary_path = wd_path + "\\data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered_with_id.csv"
final_dictionary_df = pd.read_csv(final_dictionary_path)
df_new = pd.read_csv("data\\raw\\open_alex\\openalex_authors_ready_to_match_dict.csv")
df_new.rename(columns = {'id':'pub_id','display_name':'author_display_name','affiliation_id':'id'}, inplace = True)
df_new.drop(columns=['country_code'], inplace=True)
# Do the merge
merge_df = pd.merge(df_new, final_dictionary_df, on = "id", how = "left")
# Filter only the authors that have a grid_id_string
filtered_merge_df = merge_df[merge_df['grid_id_string'].notnull()]
# filtered_merge_df.to_csv("data\\raw\\open_alex\\openalex_authors_final.csv", index = False)

# 10. Generate a dataset for people whos second or third affiliation country code is the US but their firts affiliation is not in the previous dataset
# Open multiple files
max_naffs_01, df_01 = open_and_do_minimal_cleaning(file_path_01, filter = False, filter_value = 1)
df_01               = extract_and_filter_affs_details_mult_affs(df_01)
df_01_us            = filter_affs_2and3_US(df_01)
max_naffs_02, df_02 = open_and_do_minimal_cleaning(file_path_02, filter = False, filter_value = 1)
df_02               = extract_and_filter_affs_details_mult_affs(df_02)
df_02_us            = filter_affs_2and3_US(df_02)
max_naffs_03, df_03 = open_and_do_minimal_cleaning(file_path_03, filter = False, filter_value = 1)
df_03               = extract_and_filter_affs_details_mult_affs(df_03)
df_03_us            = filter_affs_2and3_US(df_03)
max_naffs_04, df_04 = open_and_do_minimal_cleaning(file_path_04, filter = False, filter_value = 1)
df_04               = extract_and_filter_affs_details_mult_affs(df_04)
df_04_us            = filter_affs_2and3_US(df_04)
max_naffs_05, df_05 = open_and_do_minimal_cleaning(file_path_05, filter = False, filter_value = 1)
df_05               = extract_and_filter_affs_details_mult_affs(df_05)
df_05_us            = filter_affs_2and3_US(df_05)
max_naffs_06, df_06 = open_and_do_minimal_cleaning(file_path_06, filter = False, filter_value = 1)
df_06               = extract_and_filter_affs_details_mult_affs(df_06)
df_06_us            = filter_affs_2and3_US(df_06)
# Concatenate the DataFrames
df_us_mult_affs = pd.concat([df_01_us,df_02_us,df_03_us,df_04_us,df_05_us,df_06_us], axis = 0)
# df_us_mult_affs.to_csv("data\\raw\\open_alex\\openalex_authors_multaffs_ready_to_match_dict.csv", index = False)

# 11.Merge the new dataset with the final dictionary in an iterative way to not overload the memory
    # 11.1 Open the final dictionary
final_dictionary_path = wd_path + "\\data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered_with_id.csv"
final_dictionary_df = pd.read_csv(final_dictionary_path)
    # 11.2 Create an intermediate dictionaries to match affiliationid2 and 3 separately
dict_aff1 = prepare_dict_for_temp_merge(df = final_dictionary_df.copy(),affs_num = 1)
dict_aff2 = prepare_dict_for_temp_merge(df = final_dictionary_df.copy(),affs_num = 2)
dict_aff3 = prepare_dict_for_temp_merge(df = final_dictionary_df.copy(),affs_num = 3)
    # 11.2  Open the new dataset
df_new = pd.read_csv("data\\raw\\open_alex\\openalex_authors_multaffs_ready_to_match_dict.csv")
    # 11.3 Rename the columns to make the merge
df_new.rename(columns = {'id':'pub_id','display_name':'author_display_name',
              'affiliation_id1':'id1', 'affiliation_id2':'id2','affiliation_id3':'id3'}, inplace = True)
    # 11.4 Merge the first, affiliation and drop those who have matched (they are already in the first dataset)
df_new_merged = pd.merge(df_new.copy(), dict_aff1, on = "id1", how = "left")
df_new_merged = df_new_merged[df_new_merged['dummy_col_aff1'].isnull()]
    # 11.5 Merge the second and third affiliation
df_new_merged = pd.merge(df_new_merged.copy(), dict_aff2, on = "id2", how = "left")
df_new_merged = pd.merge(df_new_merged.copy(), dict_aff3, on = "id3", how = "left")
    # 11.6 Filter only those who have matched either at the second or third affiliation
df_new_merged = df_new_merged[(df_new_merged['dummy_col_aff2'] == 1) | (df_new_merged['dummy_col_aff3'] == 1)]
    # 11.7 Work with the affiliation 2 and 3 separately for the final merge
df_new_merged_aff2 = df_new_merged[df_new_merged['dummy_col_aff2'] == 1] # Second affiliation was matched regardless of the third 
df_new_merged_aff3 = df_new_merged[(df_new_merged['dummy_col_aff3'] == 1) & (df_new_merged['dummy_col_aff2'].isnull())] # Third affiliation was matched but not the second
df_new_merged.shape[0] == df_new_merged_aff2.shape[0] + df_new_merged_aff3.shape[0] # Simple test to check if the split was done correctly
    # 11.8 Merge the final dictionary with each partitioned dataset
final_dictionary_df.rename(columns = {'id':'id2'}, inplace = True)
df_new_merged_aff2 = pd.merge(df_new_merged_aff2, final_dictionary_df, on = "id2", how = "left")
df_new_merged_aff2['affiliation_used_for_merge'] = 2
final_dictionary_df.rename(columns = {'id2':'id3'}, inplace = True)
df_new_merged_aff3 = pd.merge(df_new_merged_aff3, final_dictionary_df, on = "id3", how = "left")
df_new_merged_aff3['affiliation_used_for_merge'] = 3
    # 11.9 Concatenate the two DataFrames 
df_new_merged_final = pd.concat([df_new_merged_aff2,df_new_merged_aff3], axis = 0)

# 12. Look for potential duplicates from the previous dataset, drop them and save the final dataset
    # 12.1 Open the previous dataset
prev_df = pd.read_csv("data\\raw\\open_alex\\openalex_authors_final.csv")
prev_df = prev_df[['pub_id']]
prev_df['duplicates_test'] = 1
df_new_merged_final = pd.merge(df_new_merged_final, prev_df, on = "pub_id", how = "left")
test_df = df_new_merged_final[df_new_merged_final['duplicates_test'].notnull()]
test_df.shape[0] == 0 # Simple test to check if there are duplicates
test_df.shape[0] # Number of duplicates
    # 12.1 Drop the duplicates
df_new_merged_final = df_new_merged_final[df_new_merged_final['duplicates_test'].isnull()]
df_new_merged_final.drop(columns = ['duplicates_test'], inplace = True)
    # 12.2 Save the final dataset
# df_new_merged_final.to_csv("data\\raw\\open_alex\\openalex_authors_final_affs2and3.csv", index = False)

# 13. Generate a dataset with the number of works by year (from 2012 to 2025) for each author by calling the API
# Calling the API: Linear vs Parallel Performance
# Batch size = 100,  Parallel Process: 14  seconds, Linear Process: 34  seconds
# Batch size = 1000, Parallel Process: 142 seconds, Linear Process: 384 seconds (2.70 times slower)
authors_works_df = gen_authors_ids_to_call(wd_path = wd_path, 
                                           uni_name = 'California Institute of Technology',
                                           source = "secondary_caltech" ) # Get the authors ids to call the API
authors_ids_batch = generate_id_batches(df = authors_works_df, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 10  # Set the number of batches to call the API (1 batch = 1000 authors)
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        authors_vec = authors_ids_batch[i] # Define the authors vector
        # Linear Process (used for debug)
        # authors_work = linear_works_by_year(wd_path = wd_path,authors_vec = authors_vec)
        # Parallel Process (used to call the API)
        authors_work = parallel_works_by_year(wd_path = wd_path,authors_vec = authors_vec)
# Save the final dataframe
x = gen_final_works_by_year_csv(wd_path = wd_path)

# 14. Produce institution specific works by year DataFrame (replaced flow code with a fn)
uni_df = gen_author_works_by_institution(wd_path = wd_path,uni_name = 'Princeton University', file_name = 'princeton',data_source="Bernhard")

# 15. Generate the datasets for people whos first affiliation is Caltech
    # 15.1 Open the ready to match authors from the first affiliation
df_fauthor_us = pd.read_csv("data\\raw\\open_alex\\openalex_authors_ready_to_match_dict.csv")
df_fauthor_us.rename(columns = {'id':'PersonId','display_name':'author_display_name','affiliation_id':'id'}, inplace = True)
    # 15.2 Open the final dictionary and filter Caltech Only (ror: 05dxps055)
final_dictionary_path = wd_path + "\\data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered_with_id.csv"
final_dictionary_df = pd.read_csv(final_dictionary_path)
final_dictionary_df = final_dictionary_df[final_dictionary_df['ror'] == 'https://ror.org/05dxps055']
    # 15.3 Merge the two DataFrames
merge_df = pd.merge(df_fauthor_us, final_dictionary_df, on = "id", how = "left")
    # 15.4 Filter only the authors that have a grid_id_string
filtered_merge_df = merge_df[merge_df['grid_id_string'].notnull()]
filtered_merge_df
    # 15.5 Save the final dataset
#filtered_merge_df.to_csv("data\\raw\\open_alex\\openalex_caltech_authors_final.csv", index = False)

# 16. Generate the datasets for people whos second or third affiliation is Caltech
    # 16.1 Open the ready to match authors from the second or third affiliation
df_maffs_us = pd.read_csv("data\\raw\\open_alex\\openalex_authors_multaffs_ready_to_match_dict.csv")
df_maffs_us.rename(columns = {'id':'PersonId','display_name':'author_display_name',
            'affiliation_id1':'id1', 'affiliation_id2':'id2','affiliation_id3':'id3'}, inplace = True)
    # 16.2 Open the final dictionary and filter Caltech Only (ror: 05dxps055)
final_dictionary_path = wd_path + "\\data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered_with_id.csv"
final_dictionary_df = pd.read_csv(final_dictionary_path)
final_dictionary_df = final_dictionary_df[final_dictionary_df['ror'] == 'https://ror.org/05dxps055']
    # 16.3 Create an intermediate dictionaries to match affiliationid2 and 3 separately
dict_aff1 = prepare_dict_for_temp_merge(df = final_dictionary_df.copy(),affs_num = 1)
dict_aff2 = prepare_dict_for_temp_merge(df = final_dictionary_df.copy(),affs_num = 2)
dict_aff3 = prepare_dict_for_temp_merge(df = final_dictionary_df.copy(),affs_num = 3)
    # 16.5 Merge the first, affiliation and drop those who have matched (they are already in the first dataset)
df_new_merged = pd.merge(df_maffs_us.copy(), dict_aff1, on = "id1", how = "left")
df_new_merged = df_new_merged[df_new_merged['dummy_col_aff1'].isnull()]
    # 16.7 Merge the second and third affiliation
df_new_merged = pd.merge(df_new_merged.copy(), dict_aff2, on = "id2", how = "left")
df_new_merged = pd.merge(df_new_merged.copy(), dict_aff3, on = "id3", how = "left")
    # 16.7 Filter only those who have matched either at the second or third affiliation
df_new_merged = df_new_merged[(df_new_merged['dummy_col_aff2'] == 1) | (df_new_merged['dummy_col_aff3'] == 1)]
    # 16.8 Work with the affiliation 2 and 3 separately for the final merge
df_new_merged_aff2 = df_new_merged[df_new_merged['dummy_col_aff2'] == 1] # Second affiliation was matched regardless of the third
df_new_merged_aff3 = df_new_merged[(df_new_merged['dummy_col_aff3'] == 1) & (df_new_merged['dummy_col_aff2'].isnull())] # Third affiliation was matched but not the second
df_new_merged.shape[0] == df_new_merged_aff2.shape[0] + df_new_merged_aff3.shape[0] # Simple test to check if the split was done correctly
    # 16.9 Merge the final dictionary with each partitioned dataset
final_dictionary_df.rename(columns = {'id':'id2'}, inplace = True)
df_new_merged_aff2 = pd.merge(df_new_merged_aff2, final_dictionary_df, on = "id2", how = "left")
df_new_merged_aff2['affiliation_used_for_merge'] = 2
final_dictionary_df.rename(columns = {'id2':'id3'}, inplace = True)
df_new_merged_aff3 = pd.merge(df_new_merged_aff3, final_dictionary_df, on = "id3", how = "left")
df_new_merged_aff3['affiliation_used_for_merge'] = 3
    # 16.10 Concatenate the two DataFrames 
df_new_merged_final = pd.concat([df_new_merged_aff2,df_new_merged_aff3], axis = 0)

# 17. Look for potential duplicates from the previous dataset first affiliation dataset, drop them and save the final dataset
    # 12.1 Open the previous dataset
prev_df = pd.read_csv("data\\raw\\open_alex\\openalex_caltech_authors_final.csv")
prev_df = prev_df[['PersonId']]
prev_df['duplicates_test'] = 1
df_new_merged_final = pd.merge(df_new_merged_final, prev_df, on = "PersonId", how = "left")
test_df = df_new_merged_final[df_new_merged_final['duplicates_test'].notnull()]
test_df.shape[0] == 0 # Simple test to check if there are duplicates
test_df.shape[0] # Number of duplicates
    # 12.1 Drop the duplicates
df_new_merged_final = df_new_merged_final[df_new_merged_final['duplicates_test'].isnull()]
df_new_merged_final.drop(columns = ['duplicates_test'], inplace = True)
    # 12.2 Save the final dataset
#df_new_merged_final.to_csv("data\\raw\\open_alex\\openalex_caletch_authors_final_affs2and3.csv", index = False)

# 18. Produce the Caltech specific works by year DataFrame (the parameters in this case must remain fixed)
uni_df = gen_author_works_by_institution(wd_path = wd_path,uni_name = 'California Institute of Technology', file_name = 'caltech',data_source="Caltech")