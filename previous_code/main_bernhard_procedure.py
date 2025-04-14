import pandas as pd

wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary\\data\\raw\\aarc_openalex_match\\input_files\\Bernhard_matching_help"

papers_df = pd.read_csv(wd_path +'\\doi_papers_authors_openalex.csv', encoding='latin-1')
researchers_df = pd.read_csv(wd_path + '\\aarc_doi_pending_matching.csv')

# Function to fix names (capitalize first letter, swap first and last names)
def fix_name(name):
    name = name.title()            # Capitalize first letter of each word
    name = name.split(',')         # Split the name in two parts guided by the comma
    name = name[1] + ' ' + name[0] # Swap first and last names
    return name.strip()            # Remove leading/trailing spaces   

# Function to remove middle names (keeps only first and last names)
def remove_middle_name(name):
    parts = name.split() # Split the name into parts (words) based on spaces
    return ' '.join([parts[0], parts[-1]]) if len(parts) > 2 else name

# Function to get last name (the last part of a name)
def get_last_name(name):
    return name.split()[-1]

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

# Transform the dictionary into a DataFrame to continue building the final dictionary
person_matches_df = pd.DataFrame({
    "PersonId": list(person_matches.keys()),
    "PersonOpenAlexId": list(person_matches.values())
})
file_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary\\data\\raw\\aarc_openalex_match\\input_files\\aarc_openalex_authors_matches_berhnard_raw_file\\bernhard_procedure_author_match.csv"
person_matches_df.to_csv(file_path, index=False, encoding='utf-8')