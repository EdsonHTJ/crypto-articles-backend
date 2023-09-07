from flask import Flask, Response
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
