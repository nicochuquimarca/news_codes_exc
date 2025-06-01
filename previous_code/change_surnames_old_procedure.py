# OLD CHANGE OF SURNAME PROCEDURE #
# This piece of code was first developed in the '01_aarc_openalex_match_excercise.py' file
# DISCLAIMER IN THE 

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
aarc_oa_be_fac_n_df = pd.merge(aarc_oa_be_fac_df, n, on='id', how='left') # Merge the two DataFrames on the 'id' column
aarc_oa.check_duplicates_and_missing_values(original_df = aarc_oa_be_fac_df ,new_df = aarc_oa_be_fac_n_df, column_name = 'PersonId', check_missing_values = False)
# 1.5 Get to know how many alternative names are there and their frecuency
summ_df = n.groupby('alternative_names_num').size().reset_index(name='Counts')
total_c  = summ_df['Counts'].sum() # Calculate the total of the 'Counts' column
summ_df['share'] = summ_df['Counts'] / total_c # Add a new column 'share' that shows the share of each count with respect to the total
summ_df = summ_df.sort_values(by='share', ascending=False) # Sort the DataFrame by the 'share' column in descending order
print(summ_df) # Print the summary DataFrame
print("Decision: The test excercise will be carried on, until the alternative name 10.")

# 1.6 Unpack the alternative names in different columns and remove redundant columns (name11 to name63)
aarc_oa_be_fac_n_df['display_name_alternatives'] = aarc_oa_be_fac_n_df['display_name_alternatives'].apply(ast.literal_eval) # Parse the JSON-like strings into Python lists
# Dynamically create new columns for alternative names
max_alternatives = aarc_oa_be_fac_n_df['alternative_names_num'].max()  # Find the maximum number of alternatives
for i in range(1, max_alternatives + 1):
    aarc_oa_be_fac_n_df[f'name{i:02d}'] = aarc_oa_be_fac_n_df['display_name_alternatives'].apply(lambda x: x[i - 1] if i <= len(x) else '')
# Drop all columns in n_df that match the pattern 'name11' to 'name63'
columns_to_drop = [col for col in aarc_oa_be_fac_n_df.columns if col.startswith('name') and 11 <= int(col[4:]) <= 63]
aarc_oa_be_fac_n_df = aarc_oa_be_fac_n_df.drop(columns=columns_to_drop)

# 1.7 Create the SurnameBenchmark in which we will compare the surnames
aarc_oa_be_fac_n_df = aarc_oa.format_authors_names(df = aarc_oa_be_fac_n_df, var_name ='display_name', message=True) # Split the regular name in pieces
# Get only the surnames from the regular name and the alternative name
aarc_oa_be_fac_n_df['SurnameBenchmark'] = aarc_oa_be_fac_n_df.apply(
        lambda row: row[f'display_name_{row["display_name_tot"]}'], axis=1) # Get the surname from the OpenAlex data
aarc_oa_be_fac_n_df['SurnameBenchmark'] = aarc_oa_be_fac_n_df['SurnameBenchmark'].apply(unidecode) # Remove accents
aarc_oa_be_fac_n_df['SurnameBenchmark'] = aarc_oa_be_fac_n_df['SurnameBenchmark'].str.replace('-', '') # Remove hyphens
aarc_oa_be_fac_n_df['SurnameBenchmark'] = aarc_oa_be_fac_n_df['SurnameBenchmark'].str.replace("'", '') # Remove single quotes
aarc_oa_be_fac_n_df['SurnameBenchmark'] = aarc_oa_be_fac_n_df['SurnameBenchmark'].str.upper() # Put the surname in upper case

# Drop all columns in n_df that match the pattern 'display_name_1' to 'display_name_11'
columns_to_drop = ['display_name_1','display_name_2','display_name_3','display_name_4',
                   'display_name_5','display_name_6','display_name_7','display_name_8', 
                   'display_name_9','display_name_10','display_name_11',
                   'display_name_tot']
aarc_oa_be_fac_n_df = aarc_oa_be_fac_n_df.drop(columns=columns_to_drop)

# PERFORM THE OLD PROCEDURE #
y = gender_surname_comparison(aarc_oa_be_fac_n_df) # Run the function to get the final dataframe
# Filter women with SurnameComparison_tot > 0
y = y[(y['Gender'] == 'F') & (y['SurnameComparison_tot'] > 0)] # Filter the DataFrame to only include rows