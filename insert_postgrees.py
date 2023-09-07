import pandas as pd
import requests
import db.db as db


def main():
    def requestData(timestamp: int) -> dict:
        """Request data from the CryptoCompare API."""
        url = "https://data-api.cryptocompare.com/news/v1/article/list?to_ts={}&limit=100".format(
            timestamp)
        response = requests.get(url)
        data = response.json()
        return data

    dbts = db.get_oldest_article_published_on()
    starter_timestamp = 1692206525 if dbts == 0 else dbts

    selected_columns = ["ID", "GUID", "PUBLISHED_ON", "IMAGE_URL", "TITLE", "URL", "SOURCE_ID",
                        "BODY", "KEYWORDS", "LANG", "UPVOTES", "DOWNVOTES", "SCORE", "SENTIMENT",
                        "STATUS", "SOURCE_DATA", "CATEGORY_DATA"]

    df = pd.DataFrame(columns=selected_columns)
    current_month, current_year = None, None

    num_iterations = 1000

    for i in range(num_iterations):
        print('Current, iteration: ', i)
        data = requestData(starter_timestamp)
        new_df = pd.DataFrame(data['Data'])
        new_df = new_df[selected_columns]
        new_df['PUBLISHED_ON_DATE'] = pd.to_datetime(
            new_df['PUBLISHED_ON'], unit='s')

        # Check if the current month has changed
        new_month = new_df['PUBLISHED_ON_DATE'].min().month
        new_year = new_df['PUBLISHED_ON_DATE'].min().year

        print('Saving df...')
        db.save_articles_df(new_df)

        df = pd.DataFrame(columns=selected_columns)  # Reset DataFrame
        df = pd.concat([df, new_df], ignore_index=True)
        # Update the starter_timestamp
        # Subtracting 1 to avoid fetching the same article again
        starter_timestamp = new_df['PUBLISHED_ON'].min() - 1


if __name__ == '__main__':
    main()
