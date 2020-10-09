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

def read_sql_to_gb(engine, table_name="purchase_product_id_hour", id_name = 'product_id'):
    df = pd.read_sql_table(table_name, engine)
    gb = df.groupby(by=id_name).sum()
    print (gb.head(50))
    return gb

def rank_by_id(gb, rank_metric = "count(price)", n = 10):
    gb = gb.sort_values(by=rank_metric, ascending=False)
    hot_id_list = list(gb.index.get_level_values(0))[:n]
    hot_list = [ [hot_id_list, gb[ hot_id_list ][rank_metric] ] for id in hot_id_list]
    return hot_list

def id_time_series(hot_list, df, id_name = 'product_id'):
    time_series_by_id = {}
    for id in hot_list:
        time_series_by_id[id] = [list((df.loc[df[id_name] == id])['event_time']),
        list((df.loc[df[id_name] == id])['sum(price)'])]

    dropdown_op = []

    for id in hot_list:
        dropdown_op.append({"label":str(id), "value": id})

    # dff = df.copy()
    # dff = dff[dff["product_id"] == min(views_ts.keys())]
    # s = pd.to_numeric(dff['time_period'])
    # dff = dff.drop(columns=['time_period'])
    # dff = dff.merge(s.to_frame(), left_index=True, right_index=True)
    # print (dff)

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
#     container = "The item id chosen by user was: {}".format(option_slctd)
#
#     dff = df.copy()
#     dff = dff[dff["product_id"] == option_slctd]
#     s = pd.to_numeric(dff['time_period'])
#     dff = dff.drop(columns=['time_period'])
#     dff = dff.merge(s.to_frame(), left_index=True, right_index=True)
#
#     # Plotly Express
#     fig = px.scatter(
#         data_frame=dff,
#         x = 'time_period',
#         y = 'sum(price)',
#         color = 'product_id',
#         labels={'sum(price)': 'GMV ($)',
#         'time_period':'time since start (minutes)'},
#         template='plotly_dark'
#     )
#
#     return container, fig


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    engine = create_engine(f"postgresql://{os.environ['psql_username']}:{os.environ['psql_pw']}@10.0.0.5:5431/ecommerce")
    gb = read_sql_to_gb(engine, table_name="purchase_product_id_hour", id_name = 'product_id')
    # app.run_server(debug=True, port=8051, host="10.0.0.12")
