import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

import psycopg2

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# Database connection
pg_config ={'pg_host':'mortgagepgsql.civoxbadxkwr.us-east-1.rds.amazonaws.com',
            'pg_user':'sylviaxuinsight',
            'pg_password':'568284947Aa',
            'dbname':'pgsql',
            'port':'5432'}

conn = psycopg2.connect(database = pg_config['dbname'], \
                        user = pg_config['pg_user'], \
                        password = pg_config['pg_password'],\
                        host = pg_config['pg_host'], \
                        port = pg_config['port'])
cur = conn.cursor()
sql_query_1 = ""
cur.execute(sql_query_1)
databases = cur.fetchall()
xData = [databse[0] for database in databases]
sql_query_2 = ""
cur.execute(sql_query_2)
frequecies = cur.fetchall()
yData = [freq[0] for freq in frequncies]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Hello Dash',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='Dash: A web application framework for Python.', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    dcc.Graph(
        id='example-graph-2',
        figure={
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'CA'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': 'NY'},
            ]
            'layout': {
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'font': {
                    'color': colors['text']
                }
            }
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
