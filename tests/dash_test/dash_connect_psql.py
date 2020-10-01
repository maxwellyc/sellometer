import pandas as pd
import plotly.express as px  # (version 4.7.0)
import plotly.graph_objects as go

import dash  # (version 1.12.0) pip install dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import psycopg2
import os
from sqlalchemy import create_engine

# # ------------------------------------------------------------------------------
# # Import data from postgreSQL using psycopg2
# conn = psycopg2.connect(dbname="my_db", user=os.environ['psql_username'],\
#  password=os.environ['psql_pw'], host="localhost", port=5431) # connection string
# cur = conn.cursor() # cursor object
#
# cur.execute('''SELECT * FROM event_count''')
# for row in cur.fetchall():
#     print (row)

# Import data from postgreSQL using sqlalchemy
engine = create_engine(f"postgresql://{os.environ['psql_username']}:{os.environ['psql_pw']}@localhost:5431/my_db")
print (engine)
df = pd.read_sql_table("event_count", engine)
print (df.head(20))

df1 = df.copy()
g1 = df1.groupby(by="product_id").sum()#.sort_values(by="view_cnt")
#gb = df.groupby(by="product_id").sum()#.apply(lambda _df: _df.sort_values('view_cnt'))
#df1.sort_values(by=["view_cnt"], ascending=False)
g1.add_suffix('_Count').reset_index()
g1 = g1.sort_values(by="view_cnt", ascending=False)
print (list(g1['product_id']))

# # dash Application
# app = dash.Dash(__name__)
#
# # ------------------------------------------------------------------------------
# # App layout
# app.layout = html.Div([
#
#     html.H1("Web Application Dashboards with Dash", style={'text-align': 'center'}),
#
#     dcc.Dropdown(id="slct_year",
#                  options=[
#                      {"label": "2015", "value": 2015},
#                      {"label": "2016", "value": 2016},
#                      {"label": "2017", "value": 2017},
#                      {"label": "2018", "value": 2018}],
#                  multi=False,
#                  value=2015,
#                  style={'width': "40%"}
#                  ),
#
#     html.Div(id='output_container', children=[]),
#     html.Br(),
#
#     dcc.Graph(id='my_bee_map', figure={})
#
# ])
#
#
# # ------------------------------------------------------------------------------
# # Connect the Plotly graphs with Dash Components
# @app.callback(
#     [Output(component_id='output_container', component_property='children'),
#      Output(component_id='my_bee_map', component_property='figure')],
#     [Input(component_id='slct_year', component_property='value')]
# )
# def update_graph(option_slctd):
#     print(option_slctd)
#     print(type(option_slctd))
#
#     container = "The year chosen by user was: {}".format(option_slctd)
#
#     dff = df.copy()
#     dff = dff[dff["Year"] == option_slctd]
#     dff = dff[dff["Affected by"] == "Varroa_mites"]
#
#     # Plotly Express
#     fig = px.choropleth(
#         data_frame=dff,
#         locationmode='USA-states',
#         locations='state_code',
#         scope="usa",
#         color='Pct of Colonies Impacted',
#         hover_data=['State', 'Pct of Colonies Impacted'],
#         color_continuous_scale=px.colors.sequential.YlOrRd,
#         labels={'Pct of Colonies Impacted': '% of Bee Colonies'},
#         template='plotly_dark'
#     )
#
#     return container, fig
#
#
# # ------------------------------------------------------------------------------
# if __name__ == '__main__':
#     app.run_server(debug=True, port=8051, host="10.0.0.6")
