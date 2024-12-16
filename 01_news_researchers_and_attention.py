# Researchers in the Media - Data Excercise
# Objective: Create a demo for attention in the media of research articles

# Disclaimer: Many parts of this code was built using GitHub Copilot guidance

# Author: Nicolas Chuquimarca (QMUL)
# version 0.1: 2024-12-16: Explore possible attention measures

# Comments: I unsuccesfully tried to implement the facebook_scraper library
#           and also the GraphAPI (Official Meta API) to get the number of likes, shares and comments


# Pseudo Code
# 1. Identify the number of websites these articles come from
# 2. Identify the number of articles per website
# 3. Filter the articles that come from the top-25 websites
# 4. Analyze nber articles (top 1). Conclusion: these urls are not very helpful to measure attention, they direct me straight into the pdfs.
# 5. Analyze mendeley articles (top 2). Conclusion: We can get citations and number of readers from mendeley, not very helpful for attention in the media.
# 6. Analyze facebook articles (top 3). Conclusion: We can get the number of shares, comments and reactions from facebook, this is helpful for attention in the media.
# 7. Deep dive into facebook articles (Get Likes, Shares and Comments if the post is still available)
  # 7.0 Function definition
  # 7.1 Define scrping parameters
  # 7.2 Scrape the facebook posts

# 0. Packages
import pandas as pd, os, time, math
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException # Use the try & except to handle the selenium exceptions
from selenium.common.exceptions import TimeoutException       # Use the try & except to handle the selenium exceptions
from datetime import datetime                                 # Get the current date and time

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
# facebook_urls_df.to_csv("data\\raw\\facebook_articles.csv", index=False)
# Conclusion We can get the number of shares, comments and reactions from facebook, this is helpful for attention in the media.

# 7. Deep dive into facebook articles (Get Likes, Shares and Comments if the post is still available)
# 7.0 Function list and definition
  # Fn01: check_waiting_response = Check if the default cookies button appers (borrowed from the Ecuador SRI project) 
  # Fn02: get_num_likes() = Get the number of likes, handle the exceptions gracefully

# Fn01: check_waiting_response = Check if the default cookies button appers (borrowed from the Ecuador SRI project)
def check_waiting_response(wait,xpath,element_name, max_retries):
    counter     = 0  # Initialize the counter
    while counter < max_retries:
        try:
            # Attempt to locate the element
            wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            #print(f"{element_name} found, continue with the process.")
            break  # Exit the loop if the element is found
        except TimeoutException:
            counter += 1
            #print(f"{element_name} not found, attempt {counter-1} of {max_retries} failed. Retrying")
    if counter == max_retries:
        print(f"The element {element_name} was not located after {max_retries} attempts")
    return counter

# Fn02: get_num_likes = Get the number of likes, handle the exceptions gracefully
def get_num_likes(driver,xpath):
  try:
    likes_element = driver.find_element(By.XPATH, xpath)    # Find the element with the xpath
    likes_html = likes_element.get_attribute('outerHTML')      # Get the outer HTML of the element
    soup = BeautifulSoup(likes_html, 'html.parser')      # Parse the HTML content with BeautifulSoup
    likes_text = soup.find('div', {'aria-label': True})['aria-label']
  except NoSuchElementException:
    likes_text = "Not Found"
  return likes_text

# Fn03: date_time_string = Get the current date and time in a string format (borrowed from the Ecuador SRI project)
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

# Fn04: facebook_cust_like_scraper = Scrap the likes of a facebook post one url by time
def facebook_cust_like_scraper_one_url(url,wait_seconds,max_retries):
   # Open an automated browser
  driver = webdriver.Chrome()
  driver.get(url)
  wait = WebDriverWait(driver, wait_seconds)  # 10 is the timeout in seconds

  # Reject the cookies
  reject_cookies_button_xpath = "/html/body/div[4]/div[1]/div/div[2]/div/div/div/div/div[2]/div/div[1]/div[2]"
  reject_cookies_counter = check_waiting_response(wait,reject_cookies_button_xpath,"Reject Cookies Button", max_retries)
  if reject_cookies_counter == max_retries: # If the first button is not found, try the second one
    reject_cookies_button_xpath = "/html/body/div[3]/div[1]/div/div[2]/div/div/div/div/div[2]/div/div[1]/div[2]"
  reject_cookies_button = driver.find_element(By.XPATH,reject_cookies_button_xpath)
  time.sleep(wait_seconds)
  reject_cookies_button.click()

  # Check if the content is not-available
  not_av_cont_xpath = "/html/body/div[1]/div/div[1]/div/div[3]/div/div/div[1]/div[1]/div/div/div[1]/div[2]/div[1]/h2/span/text()"
  not_av_cont_xpath = "/html/body/div[1]/div/div[1]/div/div[3]/div/div/div[1]/div[1]/div/div/div[1]/div[2]/div[1]/h2/span"
  not_av_cont_counter = check_waiting_response(wait,not_av_cont_xpath,"Not Available Content", max_retries)
  # If the content is not available, set num_likes to NA and then exit the program
  if not_av_cont_counter < max_retries:
   num_likes = "Post Not Available"
   df = pd.DataFrame({'url': [url], 'num_likes': [num_likes]}) # Create a dataframe with one observation
   return df
   
  # Do not login to facebook
  x_button_xpath = "/html/body/div[1]/div/div[1]/div/div[5]/div/div/div[1]/div/div[2]/div/div/div/div[1]"
  x_button = driver.find_element(By.XPATH,x_button_xpath)
  time.sleep(wait_seconds)
  x_button.click()

  # Get the number of likes
  likes_xpath = "/html/body/div[1]/div/div[1]/div/div[3]/div/div/div[1]/div[1]/div/div/div/div/div/div/div/div/div/div/div/div/div/div[13]/div/div/div[4]/div/div/div[1]/div/div[1]/div/div[1]/span[1]/span/span/span/span/div"
  num_likes = get_num_likes(driver=driver,xpath = likes_xpath)
  print("Number of likes correctly scraped")
  # Return the number of likes
  df = pd.DataFrame({'url': [url], 'num_likes': [num_likes]}) # Create a dataframe with one observation
  return df
   
# Fn05: facebook_cust_like_scraper = Scrap the likes of a facebook post in batch
def facebook_cust_like_scraper(url_vec,wait_seconds,max_retries):
   # Get the total number of urls to scrap
  total_num_urls = str(len(url_vec))
  # Initialize empty lists to store the results
  url_and_num_likes = [] # Url and number of likes
  # Iterate over each url in the url_vector
  for current_iter, url in enumerate(url_vec, start=1):  # start=1 to start counting from 1
      # Print message to the user
      print("Currently scrapping url" + str(current_iter) + " from " + total_num_urls)
      # Call the facebook_custom function for each url and append the result
      result = facebook_cust_like_scraper_one_url(url = url, wait_seconds = wait_seconds, max_retries = max_retries)
      url_and_num_likes.append(result)
    
  # Concatenate the list of dataframes into a single dataframe
  url_and_num_likes_df = pd.concat(url_and_num_likes)

  # Get the dates to save the files
  current_time = datetime.now()
  date_string  = date_time_string(current_time = current_time)
   
  # Set the paths to store the data
  nl_path = "data\\raw\\facebook_likes_posts\\nl_"+date_string +".csv"
  url_and_num_likes_df.to_csv(nl_path, index=False)
  print("The data was correctly saved in the path: " + nl_path)
  return url_and_num_likes_df

# Fn06: gen_scrapped_urls_df = Generate the DataFrame with the scrapped urls (borrowed from the Ecuador SRI project)
def gen_scrapped_urls_final_df(wd_path,facebook_df,):
    folder_path = wd_path + "\\data\\raw\\facebook_likes_posts" # Set the folder path
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
    appended_df = pd.concat(dfs, ignore_index=True)
    # 5. Open the original DataFrame to make a left join
    merged_df = pd.merge(facebook_df, appended_df, on='url', how='left')
    # 6. Save only those that have been scrapped
    merged_df = merged_df[merged_df['num_likes'].notna()]
    merged_df_path = wd_path + "\\data\\final\\facebook_articles_num_likes.csv"
    merged_df.to_csv(merged_df_path, index=False)    
    print("The final DataFrame was correctly generated")
    return merged_df

# Fn07: gen_urls_to_scrap_df = Generate the DataFrame to scrap (borrowed from the Ecuador SRI project)
def gen_urls_to_scrap_df(wd_path):
    # Open all the facebook urls
    fb_urls_path = wd_path + "\\data\\raw\\facebook_articles.csv"
    fb_urls_df  = pd.read_csv(fb_urls_path)
    
    # Open the final_df to check if the url is already in the final_df
    fb_scrapped_urls_path = wd_path + "\\data\\final\\facebook_articles_num_likes.csv"
    fb_scrapped_urls_df = pd.read_csv(fb_scrapped_urls_path)
    fb_scrapped_urls_df['dummy'] = 1

    # Do the merge and get the urls that are not in the final_df
    pending_to_scrap_df = pd.merge(fb_urls_df, fb_scrapped_urls_df, on = "url", how = "left")
    pending_to_scrap_df = pending_to_scrap_df[pending_to_scrap_df['num_likes'].isna()]
    # Export only the url
    pending_to_scrap_df = pending_to_scrap_df[['url']]
    return pending_to_scrap_df

# Fn08: generate_scrap_batches = Divide the Dataframe into the scrap batches (borrowed from the Ecuador SRI project)
def generate_scrap_batches(df):
    # Parameters
    num_rows = len(df)                  # Number of rows in the DataFrame
    num_batches = math.ceil(num_rows/5) # Number of batches to scrap
    ruc_vector_list = []                # List to store the RUC vectors
    
    for i in range(num_batches):
        if i == num_batches-1:
            ruc_vector = df.iloc[i*5:num_rows, 0]
        else:
            ruc_vector = df.iloc[i*5:i*5+5, 0]
        ruc_vector_list.append(ruc_vector)
    # Print an informative message on how many batches are pending to scrap
    ruc_vector_len = len(ruc_vector_list)
    print(f"The DataFrame has been divided into {ruc_vector_len} batches")
    
    return ruc_vector_list

    
# 7.1  Define scrping parameters
urls_to_scrap = gen_urls_to_scrap_df(wd_path = wd_path)  # Pending urls to scrap
batches = generate_scrap_batches(df = urls_to_scrap)     # Divide the urls into batches
wait_seconds = 1
max_retries = 3
num_batches = len(batches)

# 7.2  Scrape the facebook posts
for i in range(0,num_batches):
        print("Current batch: ", i)
        url_vect = batches[i]
        x = facebook_cust_like_scraper(url_vec = url_vect, wait_seconds = wait_seconds, max_retries = max_retries)
# Save the final DataFrame
y = gen_scrapped_urls_final_df(facebook_df= facebook_df, wd_path = wd_path)


########### Not working code (I did not have time to fix it) ###########
# Get the number of shares(Not currently working)
#num_shares_xpath = "/html/body/div[1]/div/div[1]/div/div[3]/div/div/div[1]/div[1]/div/div/div/div/div/div/div/div/div/div/div/div/div/div[13]/div/div/div[4]/div/div/div[1]/div/div[1]/div/div[2]/div[2]/span/div/div/div[1]"
#num_shares_element = driver.find_element(By.XPATH, num_shares_xpath) # Find the element with the xpath
#num_shares_html = num_shares_element.get_attribute('outerHTML')      # Get the outer HTML of the element
#soup = BeautifulSoup(num_shares_html, 'html.parser')      # Parse the HTML content with BeautifulSoup
#num_shares_text =  soup.find('span').text
#num_shares_text
# Get the numner of comments (Not currently working)
#comments_xpath = "/html/body/div[1]/div/div[1]/div/div[3]/div/div/div[1]/div[1]/div/div/div/div/div/div/div/div/div/div/div/div/div/div[13]/div/div/div[4]/div/div/div[1]/div/div[1]/div/div[2]/div[1]/span/div/div/div[1]/span/span"
#comments_element = driver.find_element(By.XPATH, comments_xpath)    # Find the element with the xpath
#comments_html = comments_element.get_attribute('outerHTML')      # Get the outer HTML of the element
#soup = BeautifulSoup(comments_html, 'html.parser')      # Parse the HTML content with BeautifulSoup
#comments_text =  soup.find('span').text


