import pandas as pd
import mongodb.mongodb as mdb
import json
from dotenv import load_dotenv


def main():
    load_dotenv()
    readedData = pd.read_csv('./estratificado.csv', delimiter=';')
    print(readedData.head())

    db = mdb.Database()

    for idx, row in readedData.iterrows():

        try:

            cat = []
            try:
                cat = row['CATEGORY_DATA_filtred'].split(':')
            except:
                pass

            data: dict = {
                '_id': row['ID'],
                'title': row['TITLE'],
                'sentiment': row['SENTIMENT'],
                'klv_sentiment': 'undefined',
                'category': cat,
            }

            db.insert_one(data)
            print('Inserted: ', idx)
        except Exception as e:
            print('Error: ', e)
            print('Error in: ', idx)
            continue


if __name__ == '__main__':
    main()
