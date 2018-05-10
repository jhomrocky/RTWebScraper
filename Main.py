from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
# BigQuery upload
from google.cloud import bigquery


def scrape_webpage():
    review_id = []
    review_text = []
    review_date = []
    review_score = []
    reviewer_name = []

    # for finding how many pages of reviews there are for the movie (in case more are added in the future)
    review_index_url = requests.get("https://www.rottentomatoes.com/m/the_wizard_of_oz_1939/reviews/")
    # loads index page into variable
    page_count_checker = BeautifulSoup(review_index_url.text, "html.parser")
    # finds the "pageInfo" class which contains "Page 1 of X" where x = last page number)
    page_count = page_count_checker.find(class_="pageInfo")
    # finds the last entry (so "6" in "Page 1 of 6"), converts to int for use in following code
    page_count = int(page_count.string[-1:])
    # used for appending review ID's later
    review_id_number = 0

    for page in range(1, page_count+1):  # has to be 7 since range is exclusive of the second number
        # index needs ?page=1 in this instance since changing pages is handled via GET instead of POST
        page_number_url = "https://www.rottentomatoes.com/m/the_wizard_of_oz_1939/reviews/?page={}&sort=".format(page)
        # loading index page into variable
        webpage = requests.get(page_number_url)
        # makin' a beautiful bowl of page soup
        page_soup = BeautifulSoup(webpage.text, "html.parser")
        # grabs all the "review" containers
        reviews = page_soup.find_all(class_="review_table_row")
        for review in reviews:
            # for giving a review an ID # (starts at 0 above 1st for loop, then +1 here starts the first ID at 1)
            review_id_number += 1
            review_id.append(review_id_number)

            # for appending reviewer names
            name = review.find(class_="articleLink")
            website_name = review.find(class_="critic_name")
            if name is None:
                # this is in case the review doesn't list an author, the review website is used in place of them
                reviewer_name.append(website_name.a.text.strip())
            else:
                reviewer_name.append(name.contents[0])

            # for appending descriptions
            review_desc = review.find(class_="the_review")
            # this is in case there is no text description given on RottenTomatoes
            if review_desc.contents[0] == ", " or review_desc.contents[0] == " ":
                review_text.append("No description given")
            else:
                review_text.append(review_desc.contents[0])

            # for appending review dates
            review_submit_date = review.find(class_="review_date")
            if review_submit_date is None:
                # this is in case there is no date given on RottenTomatoes (should never happen)
                review_date.append("No date given")
            else:
                review_date.append(review_submit_date.contents[0])

            # for appending score (Rotten/Fresh)
            num_score = re.search(r'Original Score: \d*[.]?\d*\/\d{1,3}', str(review))  # finds numerical score
            letter_score = re.search(r'Original Score: [a-zA-Z](\+|-)?', str(review))  # finds letter score
            if letter_score is not None:
                # the .replace here and in the elif are replaced because using "Original Score" with regex
                # made it easier to find scores and not erroneous data from links and other sources
                # the '"' on each side of the score appending is to force Excel to not convert things like 4/4 to
                # 4-April
                review_score.append(str('"' + letter_score.group()).replace("Original Score: ", "") + '"')
            elif num_score is not None:
                review_score.append(str('"' + num_score.group()).replace("Original Score: ", "") + '"')
            else:
                review_score.append("No score listed")

            """
             # the below code can be used to change from checking for numerical/alphabetical to checking fresh/rotten
             # it was my original attempt at finding scores before I talked to Cal
             
             review_score_type = review.find(class_="fresh")
             # RT uses Fresh/Rotten, so if a rating is not Fresh, it must be Rotten
             if review_score_type is None:
                review_score.append("Rotten")
             else:
                review_score.append("Fresh")
                
            """

    # converting to DataFrame
    movie_info = pd.DataFrame({'Review ID': review_id,
                               'Name': reviewer_name,
                               'Date': review_date,
                               'Score': review_score,
                               'Description': review_text}, dtype=str)
    # setting layout of columns
    movie_info = movie_info[['Review ID', 'Name', 'Date', 'Score', 'Description']]
    # send to csv
    movie_info.to_csv('movies.csv', index=False)


scrape_webpage()

"""
This was for uploading to BigQuery

client = bigquery.Client()
filename = 'movies.csv'
dataset_id = 'WOOReviews'
table_id = 'WizardOfOzReviews'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1
job_config.autodetect = True

with open(filename, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location='US',  # Must match the destination dataset location.
        job_config=job_config)  # API request

job.result()  # Waits for table load to complete.

print('Loaded {} rows into {}:{}.'.format(
    job.output_rows, dataset_id, table_id))
"""
