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

df = pd.read_sql_table("event_count", engine)

df1 = df.copy()
g1 = df1.groupby(by="product_id").sum()#.sort_values(by="view_cnt")
#gb = df.groupby(by="product_id").sum()#.apply(lambda _df: _df.sort_values('view_cnt'))

g1.add_suffix('_Count').reset_index()
g1 = g1.sort_values(by="view_cnt", ascending=False)

top_ids = list(g1.index.get_level_values(0))[:10]

views_ts = {}
for id in top_ids:
    views_ts[id] = [list((df.loc[df['product_id'] == id])['time_period']),
    list((df.loc[df['product_id'] == id])['view_cnt'])]

dropdown_op = []

for id in views_ts:
    dropdown_op.append({"label":str(id), "value": id})

dff = df.copy()
dff = dff[dff["product_id"] == min(views_ts.keys())]
print (dff)
s = pd.to_numeric(dff['time_period'])
print (dff.columns)
dff = dff.drop(columns=['time_period'])
dff = dff.merge(s.to_frame(), left_index=True, right_index=True)
print (dff)

# # dash Application
# app = dash.Dash(__name__)
#
# # ------------------------------------------------------------------------------
# # App layout
# app.layout = html.Div([
#
#     html.H1("Web Application Dashboards with Dash", style={'text-align': 'center'}),
#
#     dcc.Dropdown(id="slct_item",
#                  options=dropdown_op,
#                  multi=False,
#                  value=min(views_ts.keys()),
#                  style={'width': "40%"}
#                  ),
#
#     html.Div(id='output_container', children=[]),
#     html.Br(),
#
#     dcc.Graph(id='views_timeseries', figure={})
#
# ])
#
#
# # ------------------------------------------------------------------------------
# # Connect the Plotly graphs with Dash Components
# @app.callback(
#     [Output(component_id='output_container', component_property='children'),
#      Output(component_id='views_timeseries', component_property='figure')],
#     [Input(component_id='slct_item', component_property='value')]
# )
# def update_graph(option_slctd):
#     print(option_slctd)
#     print(type(option_slctd))
#
#     container = "The item chosen by user was: {}".format(option_slctd)
#
#     dff = df.copy()
#     dff = dff[dff["product_id"] == option_slctd]
#     dff = pd.to_numeric(dff)
#
#     # Plotly Express
#     fig = px.line(
#         data_frame=dff,
#         x = 'time_period',
#         y = 'view_cnt',
#         color = 'product_id',
#         template='plotly_dark'
#     )
#
#     return container, fig
#
#
# # ------------------------------------------------------------------------------
# if __name__ == '__main__':
#     app.run_server(debug=True, port=8051, host="10.0.0.6")
