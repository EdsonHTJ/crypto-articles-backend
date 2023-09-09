from flask import Flask, Response, request, jsonify
import mongodb.mongodb as mdb
from flask_cors import CORS
import json
import pandas as pd

app = Flask(__name__)
CORS(app)

db = mdb.Database()


@app.route('/')
def getRandomArticle():
    doc = db.get_random()
    return json.dumps(doc)


@app.route('/download/all')
def getAllArticles():
    docs = db.get_all()
    df = pd.DataFrame(list(docs))
    csv_data = df.to_csv(index=False)

    response = Response(csv_data, content_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=data.csv"

    return response


@app.route('/insert_article', methods=['POST'])
def insert_article():
    # Extract data from the request
    data = request.json

    # Validate the data (very basic validation for the sake of this example)
    required_fields = ['id', 'title', 'sentiment', 'klv_sentiment', 'category']

    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    data_insert: dict = {
        '_id': data['id'],
        'title': data['title'],
        'sentiment': data['sentiment'],
        'klv_sentiment': data['klv_sentiment'],
        'category': data['category'],
    }

    # Insert the article into MongoDB
    try:
        db.insert_one(data_insert)
        return jsonify({"message": "Article inserted successfully"}), 201
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


@app.route('/update_article', methods=['POST'])
def update_article():
    data = request.json

    # Validate the data (very basic validation for the sake of this example)
    required_fields = ['id', 'title', 'sentiment', 'klv_sentiment', 'category']

    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    data_insert: dict = {
        '_id': data['id'],
        'title': data['title'],
        'sentiment': data['sentiment'],
        'klv_sentiment': data['klv_sentiment'],
        'category': data['category'],
    }

    # Insert the article into MongoDB
    try:
        db.update_one(data_insert)
        return jsonify({"message": "Article inserted successfully"}), 201
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500


if __name__ == '__main__':
    app.run()
