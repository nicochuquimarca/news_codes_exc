# Researchers in the Media - Data Excercise
# Objective: Create a demo for attention in the media of research articles

# Disclaimer: Many parts of this code was built using GitHub Copilot guidance

# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2024-12-16: Explore possible attention measures

# Pseudo Code
# 1. Identify the number of websites these articles come from
# 2. Identify the number of articles per website
# 3. Filter the articles that come from the top-25 websites
# 4. Analyze nber articles (top 1). Conclusion: these urls are not very helpful to measure attention, they direct me straight into the pdfs.
# 5. Analyze mendeley articles (top 2). Conclusion: We can get citations and number of readers from mendeley, not very helpful for attention in the media.
# 6. Analyze facebook articles (top 3). Conclusion: We can get the number of shares, comments and reactions from facebook, this is helpful for attention in the media.
# 7. Deep dive into facebook articles (Get Likes, Shares and Comments if the post is still available)
  # 7.0 Function definition





# 0. Packages
import pandas as pd, os
from facebook_scraper import get_posts # Library to scrape facebook posts

# 1. Working Directory
wd_path = "C:\\Users\\nicoc\\Dropbox\\Pre_OneDrive_USFQ\\PCNICOLAS_HP_PAVILION\\Masters\\Applications2023\\EconMasters\\QMUL\\QMUL_Bursary"
os.chdir(wd_path)

# 2. Open the data
file_path = "data\\raw\\altmetrics_urls.csv"
df = pd.read_csv(file_path) # Read the csv file

# 1. Identify the number of websites these articles come from
# 1.1. Extract the website from the url
df['website'] = df['url'].str.extract(r'www\.([^.]+)\.')
# 1.2 Get a list of unique websites
uws = df['website'].unique()
uws_df = pd.DataFrame(uws, columns=['website'])
num_websites = len(uws)
print(f"Number of distinct websites: {num_websites}")

# 2. Identify the number of articles per website
website_counts = df['website'].value_counts().reset_index()
website_counts = website_counts.sort_values(by='count', ascending=False)
website_counts.head(25) # Top 25 websites

# 3. Filter the articles that come from the top-25 websites
top_25_wbs = ['nber','mendeley','facebook','adb','brookings',
              'nap','reddit','businessinsider', 'urban',
              'oecd-ilibrary','msn','cgdev','forbes',
              'ifau','psychologytoday','nytimes','imes',
              'youtube','ilo','gov','fao','eurekalert','ifs',
              'washingtonpost','govinfo']
t25_df = df[df['website'].isin(top_25_wbs)]

# 4. Analyze nber articles
nber_df = df[df['website']=='nber']
nber_urls_df = nber_df[['url']]
nber_urls_df.to_csv("data\\raw\\nber_articles.csv", index=False)
# Conclusion: these urls are not very helpful to measure attention, they direct me straight into the pdfs.

# 5. Analyze mendeley articles
mendeley_df = df[df['website']=='mendeley']
mendeley_urls_df = mendeley_df[['url']]
mendeley_urls_df.to_csv("data\\raw\\mendeley_articles.csv", index=False)
# Conclusion: We can get citations and number of readers from mendeley, not very helpful for attention in the media.

# 6. Analyze facebook articles
facebook_df = df[df['website']=='facebook']
facebook_urls_df = facebook_df[['url']]
facebook_urls_df.to_csv("data\\raw\\facebook_articles.csv", index=False)
# Conclusion We can get the number of shares, comments and reactions from facebook, this is helpful for attention in the media.

# 7. Deep dive into facebook articles (Get Likes, Shares and Comments if the post is still available)

deutsche_example = "https://www.facebook.com/permalink.php?story_fbid=4853493288075587&id=646134315478193"
url = deutsche_example


