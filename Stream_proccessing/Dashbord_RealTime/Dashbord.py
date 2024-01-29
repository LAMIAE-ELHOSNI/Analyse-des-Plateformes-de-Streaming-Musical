from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pymongo

# MongoDB configuration
mongo_uri = "mongodb://localhost:27017/"
mongo_db_name = "MUSIC_App"
collection_users = "users"
collection_artists = "artists"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db_name]

# Create Dash app
app = Dash(__name__)

# Define layout
app.layout = html.Div(
    children=[
        dcc.Graph(id="user-graph"),
        html.Div(id="artist-count-card"),
        dcc.Interval(id='interval-component', interval=1 * 1000, n_intervals=0),
    ]
)

# Callbacks to update graphs
@app.callback(Output("user-graph", "figure"), [Input("interval-component", "n_intervals")])
def update_user_graph(n):
    # Retrieve user data from MongoDB (update this part as needed)
    user_data = list(db[collection_users].find({}))

    # Create a bar chart for user data
    fig = px.bar(
        user_data,
        x="Age",
        y="music_recc_rating",
        title="User Music Recommendation Ratings",
        labels={"music_recc_rating": "Recommendation Rating"},
        color="Gender",
        height=400,
    )

    return fig

@app.callback(Output("artist-count-card", "children"), [Input("interval-component", "n_intervals")])
def update_artist_count_card(n):
    # Get the total number of artists from MongoDB
    total_artists = db[collection_artists].count_documents({})

    # Create a card displaying the total number of artists
    card_content = html.Div([
        html.H3("Total Artists"),
        html.H4(total_artists),
    ], className="card")

    return card_content

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
