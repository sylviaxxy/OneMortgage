# -*- coding: utf-8 -*-
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
sql_query_1= "SELECT * FROM avg_rate_us_by_year ORDER BY originate_year ASC"
cur.execute(sql_query_1)
databases = cur.fetchall()
xData = [database[0] for database in databases]
yData = [database[1]  for database in databases]
conn.commit()
conn.close()
cur.close()

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='One Mortage',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='Average Interest Rate vs Year.', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    dcc.Graph(
        id='avg_rate-graph-1',
        figure={
            'data': [
                {'x': xData, 'y': yData, 'name': 'Average Interest Rate','type': 'scatter', 'mode': 'lines+markers'}
            ],
            'layout': {
                'titie': 'Average Interest Rate vs year',
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
    app.run_server(debug=True, host='0.0.0.0',port=80)
