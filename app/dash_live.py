import pandas as pd
import plotly.express as px  # (version 4.7.0)
import plotly.graph_objects as go

import plotly
import dash  # (version 1.12.0) pip install dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from collections import deque
import random


import psycopg2
import os
from sqlalchemy import create_engine

def read_sql_to_df(engine, table_name="purchase_product_id_hour", id_name = 'product_id'):
    df = pd.read_sql_table(table_name, engine)
    df_gb = df.groupby(by=[id_name]).sum()
    return df, df_gb

def rank_by_id(df_gb, rank_metric = "count(price)", n = 10):
    df_gb = df_gb.sort_values(by=rank_metric, ascending=False)
    hot_id_list = list(df_gb.index.get_level_values(0))[:n]
    hot_list = [ (id, df_gb.loc[id, rank_metric]) for id in hot_id_list]
    return hot_list

def id_time_series(hot_list, df, id_name = 'product_id'):
    df_by_id, dropdown_op = {}, []
    for id, metric in hot_list:
        df_by_id[id] = df[ df[id_name] == id ]
        df_by_id[id].sort_values(by='event_time', inplace=True)
        # print (id, "\n" , df_by_id[id])
        dropdown_op.append({"label":f"{id_name}: {id}", "value": id})
    return df_by_id, dropdown_op

def update_df():
    engine = create_engine(f"postgresql://{os.environ['psql_username']}:{os.environ['psql_pw']}@10.0.0.5:5431/ecommerce")
    df, df_gb = read_sql_to_df(engine, table_name="purchase_product_id_minute", id_name = 'product_id')
    df_hour, df_gb_hour = read_sql_to_df(engine, table_name="purchase_product_id_minute", id_name = 'product_id')
    hot_list = rank_by_id(df_gb_hour, rank_metric = "count(price)", n = 10)
    df_by_id, dropdown_op = id_time_series(hot_list, df, id_name = 'product_id')
    return df_by_id, hot_list, df

df_by_id, hot_list, df = update_df()

# # dash Application
app = dash.Dash(__name__)

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H1("Sellometer", style={'text-align': 'center'}),
    dcc.Graph(id='live-ranks',animate=True),
    html.Br(),
    html.Br(),
    dcc.Input(id="p_id",
    type="number",
    placeholder="Enter Product Id",
    debounce=True,
    value=hot_list[0][0]
    ),
    html.Br(),
    dcc.Graph(id='live-graph', animate=True),
    dcc.Interval(
        id='graph-update',
        interval=90*1000,
        n_intervals=0
    ),
])
@app.callback([Output('live-ranks', 'figure'),
               Output('live-graph', 'figure')
              ],
              [Input('graph-update', 'n_intervals'),
              Input('p_id','value')
              ]
)
def update_graph_scatter(n, p_id):
    df_by_id, hot_list, df = update_df()
    df = df[df['product_id'].isin([id for id, m in hot_list])].astype({"product_id":str})

    # Plotly Go
    trace = plotly.graph_objs.Scatter(
        x = plot_df['event_time'],
        y = plot_df['sum(price)'],
        name='Sales over time',
        # labels={'sum(price)': 'GMV ($)',
        # 'time_period':'Time'},
        mode='lines+markers'
    )

    barchart = px.bar(
        data_frame= df[df['product_id'].isin([id for id, m in hot_list])],
        x = "product_id",
        y = "count(price)",
        orientation = "h",
        barmode='relative',
        labels={"product_id":"Product Id",
        "count(price)":"Quantity sold in past hour"},

    )

    return barchart, {'data': [trace],
            'layout': go.Layout(
                xaxis=dict(range=[plot_df['event_time'].min(), plot_df['event_time'].max()]),
                yaxis=dict(range=[0, plot_df['sum(price)'].max()*1.3]))
            }
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True, port=8051, host="10.0.0.12")
