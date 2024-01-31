from flask import Flask, request, jsonify
import json
from flask_caching import Cache
import pandas as pd

app = Flask(__name__)

cache = Cache(app, config={'CACHE_TYPE': 'simple'})

# Load user data
users_df = pd.read_json(
    path_or_buf=r'C:\Users\Youcode\Desktop\Analyse_Des_Plateformes_De_Streaming_Musical\repo\Analyse-des-Plateformes-de-Streaming-Musical\Data_Ingestion\DATA\user\users_data.json',
    orient='records'
)

@cache.cached(timeout=60)
@app.route('/api/users', methods=['GET'])
def get_user():
    shape = users_df.shape[0]
    _id = int(request.args.get('id', 1))

    print(shape)

    if shape <= _id:
        return jsonify("user not found")

    user_row = users_df.iloc[_id]
    response = user_row.to_json()
    return json.loads(response)

# Load artist data
artists_df = pd.read_json(
    path_or_buf=r'C:\Users\Youcode\Desktop\Analyse_Des_Plateformes_De_Streaming_Musical\repo\Analyse-des-Plateformes-de-Streaming-Musical\Data_Ingestion\DATA\artiste\artist_data_v.json',
    orient='records')

@cache.cached(timeout=60)
@app.route('/api/artist', methods=['GET'])
def get_artist():
    shape = artists_df.shape[0]
    _id = int(request.args.get('id', 1))

    print(shape)

    if shape <= _id:
        return jsonify("artist not found")

    artist_row = artists_df.iloc[_id]
    response = artist_row.to_json()
    return json.loads(response)

if __name__ == '__main__':
    app.run(debug=True)
