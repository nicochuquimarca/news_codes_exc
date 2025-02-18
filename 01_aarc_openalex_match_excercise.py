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

# Function List
# Fn01: format_dictionary                   = Format the institution dictionary to merge with the faculty list
# Fn02: check_duplicates_and_missing_values = Check for duplicates and missing values in the merge
# Fn03: format_faculty                      = Replace Institutions without an OpenAlexId with the OpenAlexId of the parent institution
# Fn04: reorder_columns                     = Set a column in a specific position
# Fn05: get_paper_authors                   = Get the authors openalex ids and names from a given paper
# Fn06: process_paper_authors               = Function that enables multiple workers to call the get_paper_authors function
# Fn07: date_time_string                    = Get the current date and time in a string format
# Fn08: linear_papers_authors               = Linear process to get the authors of a list of papers
# Fn09: parallel_papers_authors             = Parallel process to get the authors of a list of papers
# Fn10: gen_final_papers_authors_csv        = Generate the aggregate papers authors csv
# Fn11: gen_papers_doi_to_call              = Generate the papers doi to call the API
# Fn12: generate_scrap_batches              = Divide the Dataframe into the scrap batches

# Pseudo Code
# 1. Set Working directory
# 2. Merge Institution Dictionary and BusinessEconFaculty to get the OpenAlexId for the workplace and degree institution of the author
# 3. Get the OpenAlex PaperId and AuthorId from a list of papers using the doi code 
# 4. Start a doi-author_name match exercise

# 0. Packages
import os, pandas as pd, ast, requests, math   # Import the regular packages
from datetime import date                      # Get the current date
from datetime import datetime                  # Get the current date and time
import concurrent.futures, string              # For parallel processing
import time                                    # For time measurement
from unidecode import unidecode                # For string manipulation

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
def check_duplicates_and_missing_values(original_df,new_df,column_name):
    original_nrows = original_df.shape[0] 
    new_nrows      = new_df.shape[0] 
    assert original_nrows == new_nrows, "Error: There are duplicates in the merge"
    # S2. Check for missing values in a specified column
    missing_values = new_df[column_name].isnull().sum()
    assert missing_values == 0, "Error: There are " + str(missing_values) + " missing values in the merge, please do some checks"
    # S3. S1 and S2 are correct, print a message and return None
    print("The merge is correct, there are no duplicates or missing values")
    return None

# Fn03: format_faculty = Replace Institutions without an OpenAlexId with the OpenAlexId of the parent institution
def format_faculty(df):
    # S.0 Cheat Sheet
    #  Id	  InstitutionName 		                       ParentId  ParentInstitutionName
    #  369	  Rutgers - Newark                        ---> 177       Rutgers, The State University of New Jersey			
    #  235	  University of Texas-Pan American, The   ---> 557985    The University of Texas Rio Grande Valley
    #  539130 University of Texas at Brownsville, The ---> 557985    The University of Texas Rio Grande Valley
    #  537408 Rutgers University-Camden	              ---> 177       Rutgers, The State University of New Jersey
    
    # S1. Replace InstitutionId 
    df.loc[df['InstitutionId'] == 369, 'InstitutionId'] = 177.0       # Rutgers - Newark ---> Rutgers, The State University of New Jersey
    df.loc[df['InstitutionId'] == 235, 'InstitutionId'] = 557985.0    # University of Texas-Pan American, The ---> The University of Texas Rio Grande Valley
    df.loc[df['InstitutionId'] == 539130, 'InstitutionId'] = 557985.0 # University of Texas at Brownsville, The ---> The University of Texas Rio Grande Valley
    df.loc[df['InstitutionId'] == 537408, 'InstitutionId'] = 177.0    # Rutgers University-Camden ---> Rutgers, The State University of New Jersey
    # S2. Replace DegreeInstitutionId
    df.loc[df['DegreeInstitutionID'] == 369,    'DegreeInstitutionID'] = 177.0    # Rutgers - Newark ---> Rutgers, The State University of New Jersey
    df.loc[df['DegreeInstitutionID'] == 235,    'DegreeInstitutionID'] = 557985.0 # University of Texas-Pan American, The ---> The University of Texas Rio Grande Valley
    df.loc[df['DegreeInstitutionID'] == 539130, 'DegreeInstitutionID'] = 557985.0 # University of Texas at Brownsville, The ---> The University of Texas Rio Grande Valley
    df.loc[df['DegreeInstitutionID'] == 537408, 'DegreeInstitutionID'] = 177.0    # Rutgers University-Camden ---> Rutgers, The State University of New Jersey
    
    # S3. Return the formatted faculty
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

# Fn06: process_paper_authors = Function that enable multiple workers to call the get_paper_authors function
def process_paper_authors(doi):
    # 3 workers --> time sleep = 0.05
    # 4 workers --> time sleep = 0.20
    time.sleep(0.06) # Introduce a small delay between requests
    return get_paper_authors(doi)

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

# Fn08: linear_papers_authors = Linear process to get the authors of a list of papers
def linear_paper_authors(wd_path,doi_vec):
    # Linear process
    papers_list = []
    # Iterate over the author_ids
    for current_iter, doi in enumerate(doi_vec, start=1):  # start=1 to start counting from 1
        # Get the df of the current id
        df_paper = get_paper_authors(doi)
        # Append the df to the list
        papers_list.append(df_paper)
    # Concatenate the list of dataframes
    linear_papers_df = pd.concat(papers_list, axis = 0)
    # Save the DataFrame
    # Get the dates to save the files
    current_time = datetime.now()
    date_string  = date_time_string(current_time = current_time)
    path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors\\doi_papers_authors_"+date_string+".csv"
    linear_papers_df.to_csv(path, index = False)
    print("Papers information has been saved in the following path: ", path)
    # Return the DataFrame
    return linear_papers_df

# Fn09: parallel_papers_authors = Parallel process to get the authors of a list of papers
def parallel_paper_authors(wd_path,doi_vec):
    # Parallel process, limited to 3 workers due to API response constraints
    # Initialize an empty list to store the results
    papers_list = []
    # Use ThreadPoolExecutor to parallelize the API requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Map the process_paper_authors function to the doi_vec
        results = list(executor.map(process_paper_authors, doi_vec))
    # Filter out None or empty DataFrames
    papers_list = [df for df in results if df is not None and not df.empty]
    # Concatenate the list of dataframes
    parallel_papers_df = pd.concat(papers_list, axis=0)
    current_time = datetime.now()
    date_string  = date_time_string(current_time = current_time)
    path = wd_path +"\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors\\doi_papers_authors_"+date_string+".csv"
    parallel_papers_df.to_csv(path, index = False)
    print("Papers information has been saved in the following path: ", path)
    # Return the DataFrame
    return parallel_papers_df

# Fn10: gen_final_papers_authors_csv = Generate the aggregate papers authors csv
def gen_final_papers_authors_csv(wd_path):
    folder_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors"
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
    final_df = final_df.drop(columns = ['paper_author_position'])
    final_df = final_df.drop_duplicates()
    # 6. Save the final DataFrame
    final_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
    final_df.to_csv(final_file_path, index = False)
    print("The final institutions information has been saved successfully")
    return final_df

# Fn11: gen_papers_doi_to_call = Generate the papers doi to call the API
def gen_papers_doi_to_call(wd_path,source):
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
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
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

# Fn12: generate_scrap_batches = Divide the Dataframe into the scrap batches
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

# Fn13: format_authors_names = Format the author names previous to match by doi-author_name
def format_authors_names(df,var_name):
    # S1. Split the 'author_display_name' column into multiple columns
    names_split = df[var_name].str.split(' ', expand=True)
    # S2. Convert all strings in the split columns to uppercase
    names_split = names_split.apply(lambda x: x.str.upper())
    # S3. Remove all punctuation points from the strings
    names_split = names_split.apply(lambda x: x.str.replace(f"[{string.punctuation}]", "", regex=True))
    # S.4 Rename the new columns
    num_cols = names_split.shape[1]
    names_split.columns = [f'{var_name}_{i+1}' for i in range(num_cols)] 
    print(f'The max. number of name components in this dataset is {num_cols} components')
    # S.5 Count the number of non-None values in each row
    names_split[f'{var_name}_tot'] = names_split.notna().sum(axis=1)
    # S.6 Concatenate the new columns with the original DataFrame
    df = pd.concat([df, names_split], axis=1)
    # S.8 Return the formatted DataFrame
    return df

# Fn14. prepare_names_for_merge = Prepare the names for the merge, based on the number of components
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

# Fn15: merge_and_save_dfs = Merge the original and open Alex DataFrames and save the matches
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

# Fn16: get_authors_surnames(df) = Get the surnames of the authors
def filter_df_and_test_surnames(df):
    # 1. Get relevant columns and remove duplicates
    df.rename(columns = {'aarc_personid':'PersonId','aarc_name':'PersonName',
                          'author_id':'PersonOpenAlexId','author_display_name':'PersonOpenAlexName'}, inplace = True)
    df = df[['PersonId','PersonName','PersonOpenAlexId','PersonOpenAlexName']]
    df = df.drop_duplicates()
    # 2. Get the surnames
    df = format_authors_names(df = df, var_name ='PersonName') # Split the names in pieces
    df.rename(columns = {'PersonName_1':'PersonSurname'}, inplace = True) # Rename the column that contains the surname
    df = format_authors_names(df = df, var_name ='PersonOpenAlexName') # Split the names in pieces
    df['PersonSurnameOpenAlex'] = df.apply(
        lambda row: row[f'PersonOpenAlexName_{row["PersonOpenAlexName_tot"]}'], axis=1) # Get the surname from the OpenAlex data
    df = df[['PersonId','PersonName','PersonOpenAlexId','PersonOpenAlexName','PersonSurname','PersonSurnameOpenAlex']] # Select only relevant columns
    df['PersonSurnameOpenAlex'] = df['PersonSurnameOpenAlex'].apply(unidecode) # Remove accents
    df['PersonSurnameOpenAlex'] = df['PersonSurnameOpenAlex'].str.replace('-', '') # Remove hyphens
    df['SurnameMatch'] = df['PersonSurname'] == df['PersonSurnameOpenAlex']
    # 3. Return the DataFrame
    return df

# Fn17: gen_final_aarc_openalex_authors_dictionary = Generate the final aarc openalex authors file and the authors dictionary
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
    matches_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_final.csv"
    matches_df.to_csv(matches_file_path, index = False)
    print("The final institutions information has been saved successfully")
    # Now we will create a dictionary that tells me the authors that have been and have not been matched
    # 7. Open the FacultyList
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\BusinessEconFacultyLists_OpenAlexIds.csv"
    df = pd.read_csv(fp)
    df = df[['PersonId','PersonName']]
    df.drop_duplicates(inplace = True)
    df['PersonId'] = df['PersonId'].astype(str)
    # 8. Remove PersonName to avoid _x and _y columns
    matches_df = matches_df[['PersonId','PersonOpenAlexId', 'PersonOpenAlexName']]
    # 9. Merge the two DataFrames
    df2 = pd.merge(df, matches_df, on = 'PersonId', how = 'left')
    df2['matched'] = df2['PersonOpenAlexId'].notnull()
    # 10. Save the final DataFrame
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_dictionary.xlsx"
    df2.to_excel(file_path, index = False)
    print("The author dictionary has been saved successfully")
    # 11. Return the final DataFrame
    return df2

# Fn18: get_pending_authors = Get the authors that have not been matched from previous iterations
def get_pending_authors(wd_path,df):
    # 1. Open the final dictionary
    fp = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\aarc_openalex_author_dictionary.xlsx"
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


# 1. Set Working directory
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)
pd.set_option('display.max_columns', None)

# This should be the last step after the author dictionary is completed
# 2. Merge Institution Dictionary and BusinessEconFaculty to get the OpenAlexId for the workplace and degree institution of the author
    # 2.1 Open the Dictionary and BusinessEconFaculty files
folder_path   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\"
faculty_df    = pd.read_csv(folder_path + "BusinessEconFacultyLists.csv")
dictionary_df = pd.read_csv(folder_path + "Final_sample_AARC_IPEDS_filtered_with_id.csv")
    # 2.2 Format the BusinessEconFaculty to merge with the dictionary
faculty_df = format_faculty(faculty_df)
    # 2.3 Format the dictionary to merge with the faculty
dict1_df = format_dictionary(dictionary_df,'Institution')
dict2_df = format_dictionary(dictionary_df,'DegreeInstitution')
    # 2.4 Merge the faculty with the dictionary (Institution)
faculty_new_df = faculty_df.copy() # Copy the faculty list (to avoid making changes on the original)
faculty_new_df = pd.merge(faculty_new_df, dict1_df, on = "InstitutionId", how = "left") # Merge with the institution
check_duplicates_and_missing_values(original_df = faculty_df, new_df = faculty_new_df, column_name='InstitutionOpenAlexId')
    # 2.5 Merge the faculty with the dictionary (DegreeInstitution)
faculty_new_df = pd.merge(faculty_new_df, dict2_df, on = "DegreeInstitutionID", how = "left") # Merge with the degree institution
check_duplicates_and_missing_values(original_df = faculty_df, new_df = faculty_new_df, column_name='InstitutionOpenAlexId')
    # 2.6 Reorder columns to match aarc ids with openalex ids
faculty_new_df.columns
faculty_new_df = reorder_columns(df = faculty_new_df, column_name = 'InstitutionOpenAlexId', position = 5)
faculty_new_df = reorder_columns(df = faculty_new_df, column_name = 'InstitutionOpenAlexName', position = 7)
faculty_new_df = reorder_columns(df = faculty_new_df, column_name = 'DegreeInstitutionOpenAlexId', position = 14)
faculty_new_df = reorder_columns(df = faculty_new_df, column_name = 'DegreeInstitutionOpenAlexName', position = 16)
    # 2.7 Save the new faculty list
folder_path = wd_path + "\\data\\raw\\aarc_openalex_match\\output_files\\"
# faculty_new_df.to_csv(folder_path + "BusinessEconFacultyLists_OpenAlexIds.csv", index = False)

# 3. Get the OpenAlex PaperId and AuthorId from a list of papers using the doi code 
# Time per batch: 178 seconds, 2 minutes and 58 seconds --> approx 3 minutes
# Time to run all the 234 batches: 12 hours and 42 minutes
    # 2.1 Generate the papers to call
papers_to_call = gen_papers_doi_to_call(wd_path,source='aarc_yusuf') # Generate the papers to call
papers_doi_batch =  generate_id_batches(df = papers_to_call, batch_size = 1000) # Transform the DataFrame into batches
num_batches = 58  # Set the number of batches to call the API (1 batch = 1000 papers)
    # 2.2 Call the API to get the authors openalex ids and names
for i in range(0,num_batches):             # Iterate over each batch
        print("Current batch: ", i)        # Print the current batch to the user
        doi_vec = papers_doi_batch[i] # Define the authors vector
        # Linear Process (used for debug)
        # paper_authors_df = linear_paper_authors(wd_path,doi_vec)
        # Parallel Process (used to call the API)
        paper_authors_df = parallel_paper_authors(wd_path,doi_vec)
    # 2.3 Save the final dataframe
z = gen_final_papers_authors_csv(wd_path = wd_path)

# 4. Doi-author_name match exercise
# 4.1 Original data
fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
df1 = pd.read_excel(fp)
df1 = df1[['aarc_personid','doi','aarc_name']]
df1 = df1.drop_duplicates()
# 4.2 OpenAlex data
fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
df2 = pd.read_csv(fp)
df2 = df2[df2['api_found']=="Yes"] # Filter only papers with information
df2 = df2[['paper_doi','paper_num_authors']] # Select the most basic columns
df2 = df2.drop_duplicates()
# 4.3 Merge and create the inputs for each iteration
df3 = pd.merge(df1, df2, left_on = 'doi', right_on = 'paper_doi', how = 'left')
# 4.4 Create multiple DataFrames using the number of authors in the papers
one_author_papers_df   = df3[df3['paper_num_authors']==1] # Iteration 1
two_author_papers_df   = df3[df3['paper_num_authors']==2] # Iteration 2
three_author_papers_df = df3[df3['paper_num_authors']==3] # Iteration 3
four_author_papers_df  = df3[df3['paper_num_authors']==4] # Iteration 4
five_author_papers_df  = df3[df3['paper_num_authors']==5] # Iteration 5
six_author_papers_df   = df3[df3['paper_num_authors']==6] # Iteration 6
# 4.5 Open the OpenAlex data again and get the author information
fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
df2 = pd.read_csv(fp)
df2 = df2[['paper_doi','author_id','author_display_name','paper_raw_author_name']] 
df2 = df2.drop_duplicates()
# 4.6 Start the iterations
    # 4.6.1 Iteration 1: Papers with one author only (one_author_papers_df)
        # Step1: Merge by doi to get the authors information    
one_author_papers_id_df = pd.merge(one_author_papers_df, df2, on = 'paper_doi', how = 'left')
        # Step 2. Select columns, test the surnames of the authors and save only those who match
one_author_papers_id_df = filter_df_and_test_surnames(one_author_papers_id_df)
one_author_papers_id_match_df   = one_author_papers_id_df[one_author_papers_id_df['SurnameMatch']==True]
one_author_papers_id_nomatch_df = one_author_papers_id_df[one_author_papers_id_df['SurnameMatch']==False]
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_1_author_match.csv"
one_author_papers_id_match_df.to_csv(file_path, index = False)  
        # Step 3. Generate the final file
x = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path)
    # 4.6.2 Iteration 2: Papers with two authors (removing the ones that have been matched in the previous iteration)
        # Step1: Merge by doi to get the authors information    
two_author_papers_id_df = pd.merge(two_author_papers_df, df2, on = 'paper_doi', how = 'left')
        # Step 2. Select columns, test the surnames of the authors and save only those who match
two_author_papers_id_df = filter_df_and_test_surnames(two_author_papers_id_df)
        # Step 3. Get the authors that have not been matched from previous iterations
two_author_papers_id_df = get_pending_authors(wd_path = wd_path, df = two_author_papers_id_df)
        # Step 4. Save only those who match (remove duplicates before saving)
two_author_papers_id_match_df   = two_author_papers_id_df[two_author_papers_id_df['SurnameMatch']==True]
two_author_papers_id_nomatch_df = two_author_papers_id_df[two_author_papers_id_df['SurnameMatch']==False]
two_author_papers_id_match_df   = two_author_papers_id_match_df.drop_duplicates()
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_2_author_match.csv"
two_author_papers_id_match_df.to_csv(file_path, index = False)  
        # Step 5. Generate the final file
x = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path)
    # 4.6.3 Iteration 3: Papers with three authors (removing the ones that have been matched in the previous iterations)
        # Step1: Merge by doi to get the authors information    
three_author_papers_id_df = pd.merge(three_author_papers_df, df2, on = 'paper_doi', how = 'left')
        # Step 2. Select columns, test the surnames of the authors and save only those who match
three_author_papers_id_df = filter_df_and_test_surnames(three_author_papers_id_df)
        # Step 3. Get the authors that have not been matched from previous iterations
three_author_papers_id_df = get_pending_authors(wd_path = wd_path, df = three_author_papers_id_df)
        # Step 4. Save only those who match (remove duplicates before saving)
three_author_papers_id_match_df   = three_author_papers_id_df[three_author_papers_id_df['SurnameMatch']==True]
three_author_papers_id_nomatch_df = three_author_papers_id_df[three_author_papers_id_df['SurnameMatch']==False]
three_author_papers_id_match_df   = three_author_papers_id_match_df.drop_duplicates()
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_3_author_match.csv"
three_author_papers_id_match_df.to_csv(file_path, index = False)  
        # Step 5. Generate the final file
x = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path)
    # 4.6.4 Iteration 4: Papers with four authors (removing the ones that have been matched in the previous iterations)
        # Step1: Merge by doi to get the authors information
four_author_papers_id_df = pd.merge(four_author_papers_df, df2, on = 'paper_doi', how = 'left')
        # Step 2. Select columns, test the surnames of the authors and save only those who match
four_author_papers_id_df = filter_df_and_test_surnames(four_author_papers_id_df)
        # Step 3. Get the authors that have not been matched from previous iterations
four_author_papers_id_df = get_pending_authors(wd_path = wd_path, df = four_author_papers_id_df)
        # Step 4. Save only those who match (remove duplicates before saving)
four_author_papers_id_match_df   = four_author_papers_id_df[four_author_papers_id_df['SurnameMatch']==True]
four_author_papers_id_nomatch_df = four_author_papers_id_df[four_author_papers_id_df['SurnameMatch']==False]
four_author_papers_id_match_df   = four_author_papers_id_match_df.drop_duplicates()
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_4_author_match.csv"
four_author_papers_id_match_df.to_csv(file_path, index = False)  
        # Step 5. Generate the final file
x = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path)

# 4.6.5 Iteration 5: Papers with five authors (removing the ones that have been matched in the previous iterations)
        # Step1: Merge by doi to get the authors information
five_author_papers_id_df = pd.merge(five_author_papers_df, df2, on = 'paper_doi', how = 'left')
        # Step 2. Select columns, test the surnames of the authors and save only those who match
five_author_papers_id_df = filter_df_and_test_surnames(five_author_papers_id_df)
        # Step 3. Get the authors that have not been matched from previous iterations
five_author_papers_id_df = get_pending_authors(wd_path = wd_path, df = five_author_papers_id_df)
        # Step 4. Save only those who match (remove duplicates before saving)
five_author_papers_id_match_df   = five_author_papers_id_df[five_author_papers_id_df['SurnameMatch']==True]
five_author_papers_id_nomatch_df = five_author_papers_id_df[five_author_papers_id_df['SurnameMatch']==False]
five_author_papers_id_match_df   = five_author_papers_id_match_df.drop_duplicates()
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_5_author_match.csv"
five_author_papers_id_match_df.to_csv(file_path, index = False)  
        # Step 5. Generate the final file
x = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path)




# 4.6.6 Iteration 6: Papers with five authors (removing the ones that have been matched in the previous iterations)
        # Step1: Merge by doi to get the authors information
six_author_papers_id_df = pd.merge(six_author_papers_df, df2, on = 'paper_doi', how = 'left')
        # Step 2. Select columns, test the surnames of the authors and save only those who match
six_author_papers_id_df = filter_df_and_test_surnames(six_author_papers_id_df)
        # Step 3. Get the authors that have not been matched from previous iterations
six_author_papers_id_df = get_pending_authors(wd_path = wd_path, df = six_author_papers_id_df)
        # Step 4. Save only those who match (remove duplicates before saving)
six_author_papers_id_match_df   = six_author_papers_id_df[six_author_papers_id_df['SurnameMatch']==True]
six_author_papers_id_nomatch_df = six_author_papers_id_df[six_author_papers_id_df['SurnameMatch']==False]
six_author_papers_id_match_df   = six_author_papers_id_match_df.drop_duplicates()
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches\\iter_6_author_match.csv"
six_author_papers_id_match_df.to_csv(file_path, index = False)  
        # Step 5. Generate the final file
x = gen_final_aarc_openalex_authors_dictionary(wd_path = wd_path)









four_author_papers_df




### PREVIOOOUS CODEEE
# EE #####
# 4. Doi-author_name match exercise
    # 4.1 Open original dataframe, select the most basic columns, and format the names to see their components
fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
o_df = pd.read_excel(fp)
o_df = o_df[['aarc_personid','doi','aarc_name']]
o_df = format_authors_names(df = o_df, var_name ='aarc_name')
    # 4.2 Open the OpenAlex data, filter valid papers, select the most basic columns, and format the names to see their components
fp   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
a_df = pd.read_csv(fp)
a_df = a_df[a_df['api_found']=="Yes"] # Filter only papers with information
a_df = a_df[['paper_doi','paper_num_authors','author_id','author_display_name','paper_raw_author_name']] # Select the most basic columns
a_df = format_authors_names(df = a_df, var_name ='author_display_name') # Format the author names
    # 4.3 Prepare the names and do the merge case by case
    # 4.3.1 Names with two components 
o_2c_df  = prepare_names_for_merge(df = o_df, type_df = 'original', var_name = 'aarc_name', num_c = 2, format_doi = False)
a_2c_df  = prepare_names_for_merge(df = a_df, type_df = 'openalex', var_name = 'author_display_name', num_c = 2, format_doi = True)
oa_2c_df = merge_and_save_dfs(df1 = o_2c_df, df2 = a_2c_df, num_components = '2')
    # 4.3.2 Names with three components (Contnue iterations)
    # 4.4 Create a final author_id - aarc_personid match file
