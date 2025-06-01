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

