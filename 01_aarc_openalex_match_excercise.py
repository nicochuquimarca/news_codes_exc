                                # Research in the Media
# Objectives: 1. Match the private data ids (AARC) with publicly available data ids (openalex)
# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2025-02-06: First version
# version 0.2: 2025-02-07: First merge (Faculty and Dictionary)

# Function List
# Fn01: format_dictionary = Format the institution dictionary to merge with the faculty list

# 0. Packages
import os, pandas as pd 

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
        df.rename(columns = {'aarc_id':'DegreeInstitutionId', 'display_name':'DegreeInstitutionOpenAlexName'}, inplace = True) # Rename the column to merge
        df['DegreeInstitutionOpenAlexId'] = df['id'].apply(lambda x: x.split('/')[-1]) # Extract the openalex id
        select_cols2 = ['DegreeInstitutionId', 'DegreeInstitutionOpenAlexId', 'DegreeInstitutionOpenAlexName'] # Select the columns to collect on the merge
        df = df[select_cols2] # Filter the columns        
        df = df[df['DegreeInstitutionId'].notnull()] # Remove Caltech (has a null id)
        df['DegreeInstitutionId'] = df['DegreeInstitutionId'].astype('int64') # Convert the id to integer to match with the faculty list
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


# 1. Working directory
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)
pd.set_option('display.max_columns', None)

# 2. Merge Institution Dictionary and BusinessEconFaculty
    # 2.1 Open the Dictionary and BusinessEconFaculty files
folder_path   = wd_path + "\\data\\raw\\aarc_openalex_match\\input_files\\"
faculty_df    = pd.read_csv(folder_path + "BusinessEconFacultyLists.csv")
dictionary_df = pd.read_csv(folder_path + "Final_sample_AARC_IPEDS_filtered_with_id.csv")
    # 2.2 Format the dictionary to merge with the faculty
dict1_df = format_dictionary(dictionary_df,'Institution')
dict2_df = format_dictionary(dictionary_df,'DegreeInstitution')
    # 2.3 Merge the faculty with the dictionary (Institution)
faculty_new_df = faculty_df.copy() # Copy the faculty list (to avoid making changes on the original)
faculty_new_df = pd.merge(faculty_new_df, dict1_df, on = "InstitutionId", how = "left") # Merge with the institution
check_duplicates_and_missing_values(original_df = faculty_df, new_df = faculty_new_df, column_name='InstitutionOpenAlexId')

# Handle special cases
special_cases =  faculty_new_df[faculty_new_df['InstitutionOpenAlexId'].isnull()]
special_cases.to_excel(folder_path + "special_cases.xlsx", index = False)