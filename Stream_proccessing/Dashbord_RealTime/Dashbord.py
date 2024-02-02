from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import pymongo
import plotly.graph_objs as go
from collections import Counter
from wordcloud import WordCloud
import base64
import io

# MongoDB configuration
mongo_uri = "mongodb://localhost:27017/"
mongo_db_name = "MUSIC_App"
collection_artists = "artists"
collection_users = "users"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_db_name]

# Create Dash app
app = Dash(__name__)

# Define layout with Bootstrap
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("Music Dashboard", className="dashboard-header"),
        html.Div(id="total-count-card", className="total-count-card")
    ], className="header-container"),

    # Main content
    html.Div([
        # Artist Information
        html.Div([
            html.H2("Artist Information", className="section-header"),
            dcc.Graph(id="top-10-artists-graph", className="graph"),
            dcc.Graph(id="artist-gender-pie", className="graph"),
            dcc.Graph(id="artist-country-pie", className="graph"),
            html.Div(id="wordcloud", className="graph"),
        ], className="section"),

        # User Information
        html.Div([
            html.H2("User Information", className="section-header"),
            dcc.Graph(id="user-gender-pie", className="graph"),
            dcc.Graph(id="user-music-time-slot-bar", className="graph"),
            dcc.Graph(id="user-country-map", className="graph"),
        ], className="section"),
    ], className="content-container"),

    # Interval for updating data
    dcc.Interval(id="interval-component", interval=10*1000, n_intervals=0)
])

# Callback to update total count card
@app.callback(Output("total-count-card", "children"), [Input("interval-component", "n_intervals")])
def update_total_count_card(n):
    total_artists = db[collection_artists].count_documents({})
    total_users = db[collection_users].count_documents({})
    return html.Div([
        html.H3("Total Artists"),
        html.H4(total_artists),
        html.H3("Total Users"),
        html.H4(total_users),
    ])

# Callback to update top 10 artists graph
@app.callback(Output('top-10-artists-graph', 'figure'), [Input('interval-component', 'n_intervals')])
def update_top_10_artists_graph(n):
    # Fetch top 10 artists based on listeners
    top_artists = db[collection_artists].find().sort([('listeners', -1)]).limit(10)
    
    # Prepare data for visualization
    x = [artist['name'] for artist in top_artists]
    y = [artist['listeners'] for artist in top_artists]

    # Create bar chart
    trace = go.Bar(x=x, y=y)
    layout = go.Layout(title='Top 10 Artists by Listeners', margin=dict(t=30, b=30, l=50, r=50))
    return {'data': [trace], 'layout': layout}

# Callback to update artist gender pie chart
@app.callback(Output('artist-gender-pie', 'figure'), [Input('interval-component', 'n_intervals')])
def update_artist_gender_pie(n):
    # Fetch artist gender distribution
    genders = [artist['gender'] for artist in db[collection_artists].find()]
    gender_counts = Counter(genders)
    
    # Convert dict_keys object to list
    labels = list(gender_counts.keys())
    values = list(gender_counts.values())
    
    # Create pie chart
    trace = go.Pie(labels=labels, values=values)
    layout = go.Layout(title='Artist Gender Distribution', margin=dict(t=30, b=30, l=50, r=50))
    return {'data': [trace], 'layout': layout}

# Callback to update artist country pie chart
@app.callback(Output('artist-country-pie', 'figure'), [Input('interval-component', 'n_intervals')])
def update_artist_country_pie(n):
    # Fetch artist country distribution
    countries = [artist['country'] for artist in db[collection_artists].find()]
    country_counts = Counter(countries)
    
    # Create pie chart
    labels = list(country_counts.keys())
    values = list(country_counts.values())
    trace = go.Pie(labels=labels, values=values)
    layout = go.Layout(title='Artist Country Distribution', margin=dict(t=30, b=30, l=50, r=50))
    return {'data': [trace], 'layout': layout}

# Callback to update wordcloud
@app.callback(Output('wordcloud', 'children'), [Input('interval-component', 'n_intervals')])
def update_wordcloud(n):
    # Fetch genres
    genres = [artist['genre'] for artist in db[collection_artists].find()]
    
    # Create word cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(' '.join(genres))
    
    # Convert word cloud to image
    img_stream = io.BytesIO()
    wordcloud.to_image().save(img_stream, format='PNG')
    encoded_image = base64.b64encode(img_stream.getvalue()).decode('utf-8')
    
    return html.Img(src='data:image/png;base64,{}'.format(encoded_image), className='wordcloud')

# Callback to update user gender pie chart
@app.callback(Output('user-gender-pie', 'figure'), [Input('interval-component', 'n_intervals')])
def update_user_gender_pie(n):
    # Fetch user gender distribution
    genders = [user['Gender'] for user in db[collection_users].find()]
    gender_counts = Counter(genders)
    
    # Create pie chart
    labels = list(gender_counts.keys())
    values = list(gender_counts.values())
    trace = go.Pie(labels=labels, values=values)
    layout = go.Layout(title='User Gender Distribution', margin=dict(t=30, b=30, l=50, r=50))
    return {'data': [trace], 'layout': layout}

# Callback to update user music time slot bar chart
@app.callback(Output('user-music-time-slot-bar', 'figure'), [Input('interval-component', 'n_intervals')])
def update_user_music_time_slot_bar(n):
    # Fetch user music time slot distribution
    time_slots = [user['music_time_slot'] for user in db[collection_users].find()]
    time_slot_counts = Counter(time_slots)
    
    # Create bar chart
    x = list(time_slot_counts.keys())
    y = list(time_slot_counts.values())
    trace = go.Bar(x=x, y=y)
    layout = go.Layout(title='User Music Time Slot Distribution', margin=dict(t=30, b=30, l=50, r=50))
    return {'data': [trace], 'layout': layout}

# Callback to update user country map chart
@app.callback(Output('user-country-map', 'figure'), [Input('interval-component', 'n_intervals')])
def update_user_country_map(n):
    # Fetch user country distribution
    countries = [user['nationality'] for user in db[collection_users].find()]
    country_counts = Counter(countries)
    
    # Create map chart
    data = [dict(
        type='choropleth',
        locations=list(country_counts.keys()),
        z=list(country_counts.values()),
        locationmode='country names',
        colorscale='Viridis',
        colorbar=dict(title='Number of Users')
    )]
    layout = dict(
        title='User Distribution by Country',
        geo=dict(showframe=False, projection=dict(type='equirectangular'))
    )
    return {'data': data, 'layout': layout}

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
