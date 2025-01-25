                                # Research in the Media
# Objectives: 1. Produce a institution dictionary that connects institution ids (id and grid_id_string) 
#             2. With the dictionary in hand, filter authors coming from institutions of interest
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-01-21: First version
# version 1.1: 2025-01-24: Search for the second and third author affiliations and check if they belong to the US and not in the previous dataset
# version 1.2: 2025-01-25: Generate the final dataset with the authors that have their second or third affiliation in the US

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

# 0. Packages
import os, pandas as pd, ast, requests, math
from datetime import date                                     # Get the current date
from datetime import datetime                                 # Get the current date and time

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
        # Print the progress
    
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
def generate_id_batches(df):
    # Parameters
    num_rows = len(df)                  # Number of rows in the DataFrame
    num_batches = math.ceil(num_rows/100) # Number of batches to scrap
    id_vector_list = []                # List to store the id vectors
    
    for i in range(num_batches):
        if i == num_batches-1:
            id_vector = df.iloc[i*100:num_rows, 0]
        else:
            id_vector = df.iloc[i*100:i*100+100, 0]
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
ids_batch = generate_id_batches(ids_to_call_df)
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
final_dictionary.to_csv("data\\raw\\open_alex\\Final_sample_AARC_IPEDS_filtered_with_id.csv", index = False)

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