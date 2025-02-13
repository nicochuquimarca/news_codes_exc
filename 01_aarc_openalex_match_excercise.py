                                # Research in the Media
# Objectives: 1. Match the private data ids (AARC) with publicly available data ids (openalex)
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-02-06: First version
# version 0.2: 2025-02-07: First merge (Faculty and Dictionary)
# version 1.2: 2025-02-10: Handle special cases and create a Economic Faculty List with OpenAlexIds on the Institutions they work and attended school
# version 2.1: 2025-02-11: Get the authors openalex ids and names
# version 2.2: 2025-02-12: Get the authors openalex ids and names (continuation)
# version 3.1: 2025-02-12: Start an doi-author_name match exercise

# Function List
# Fn01: format_dictionary = Format the institution dictionary to merge with the faculty list
# Fn02: check_duplicates_and_missing_values = Check for duplicates and missing values in the merge
# Fn03: format_faculty = Replace Institutions without an OpenAlexId with the OpenAlexId of the parent institution
# Fn04: reorder_columns = Set a column in a specific position
# Fn05: get_paper_authors = Get the authors openalex ids and names from a given paper
# Fn06: process_paper_authors = Function that enable multiple workers to call the get_paper_authors function
# Fn07: date_time_string = Get the current date and time in a string format
# Fn08: linear_papers_authors = Linear process to get the authors of a list of papers
# Fn09: parallel_papers_authors = Parallel process to get the authors of a list of papers
# Fn10: gen_final_papers_authors_csv = Generate the aggregate papers authors csv
# Fn11: gen_papers_doi_to_call = Generate the papers doi to call the API
# Fn12: generate_scrap_batches = Divide the Dataframe into the scrap batches

# Pseudo Code
# 1. Set Working directory
# 2. Merge Institution Dictionary and BusinessEconFaculty to get the OpenAlexId for the workplace and degree institution of the author
# 3. Get the OpenAlex PaperId and AuthorId from a list of papers using the doi code 
# 4. Start a doi-author_name match exercise


# 0. Packages
import os, pandas as pd, ast, requests, math   # Import the regular packages
from datetime import date                      # Get the current date
from datetime import datetime                  # Get the current date and time
import concurrent.futures                      # For parallel processing
import time                                    # For time measurement

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
    # 5. Save the final DataFrame
    final_file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
    final_df.to_csv(final_file_path, index = False)
    print("The final institutions information has been saved successfully")
    return final_df

# Fn11: gen_papers_doi_to_call = Generate the papers doi to call the API
def gen_papers_doi_to_call(wd_path):
    # S.1 Open the input data provided by Moqi
    file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
    df_papers = pd.read_excel(file_path)
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


# 1. Set Working directory
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)
pd.set_option('display.max_columns', None)

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
papers_to_call = gen_papers_doi_to_call(wd_path)
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


# 4. Start a doi-author_name match exercise
    # 4.1 Get the papers with ids
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
papers_authors_df = pd.read_csv(file_path)
sel_cols = ['paper_id', 'paper_doi', 'paper_title', 'paper_num_authors']
papers_authors_df = papers_authors_df[sel_cols]
papers_authors_df = papers_authors_df.drop_duplicates() 
    #  4.2 Rename a column for the merge
papers_authors_df.rename(columns = {'paper_doi':'doi'}, inplace = True)
    # 4.3 Get the original papers dataset
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\AARC_people_DOI.xlsx"
papers_df = pd.read_excel(file_path)
    # 4.4 Merge the two datasets
papers_authors_mdf = pd.merge(papers_df, papers_authors_df, on = "doi", how = "left")
    # 4.5 Get the authors openalex ids and names
papers_authors_valid_df = papers_authors_mdf[papers_authors_mdf['paper_id'].notnull()]
papers_authors_invalid_df = papers_authors_mdf[papers_authors_mdf['paper_id'].isnull()]  
    # 4.6 Measure the percentage of papers found by OpenAlex API
total_papers_count      = papers_df.shape[0]   
merged_papers_count     = papers_authors_valid_df.shape[0]
non_merged_papers_count = papers_authors_invalid_df.shape[0]
percentage_found = round((merged_papers_count/total_papers_count)*100,2)
percentage_not_found = round((non_merged_papers_count/total_papers_count)*100,2)
print("Percentage of obs found by OpenAlex API: ", percentage_found,"%")
    # 4.7 Open the papers_authors again and work only with the paper_authors_valid_df
file_path = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\doi_papers_authors_openalex.csv"
papers_authors_complete_df = pd.read_csv(file_path)
    # 4.8 Filter the dataframe for valid obs
papers_authors_complete_df = papers_authors_complete_df[papers_authors_complete_df['api_found']=="Yes"]
    # 4.9 Create a doi_author_name key to merge with the original dataset
papers_authors_complete_df['doi_author_name'] = papers_authors_complete_df['paper_doi'] + "_" + papers_authors_complete_df['author_display_name']

papers_authors_complete_df