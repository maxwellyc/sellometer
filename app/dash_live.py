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

def rank_by_id(df_gb, rank_metric = "count(price)", n = 20):
    df_gb = df_gb.sort_values(by=rank_metric, ascending=False)
    print (df_gb.head(20))
    hot_id_list = list(df_gb.index.get_level_values(0))[:n]
    hot_list = [ (id, df_gb.loc[id, rank_metric]) for id in hot_id_list]
    print (hot_list)
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
    hot_list = rank_by_id(df_gb_hour, rank_metric = "count(price)", n = 20)
    df_by_id, dropdown_op = id_time_series(hot_list, df, id_name = 'product_id')
    return df_by_id, hot_list, dropdown_op

df_by_id, hot_list, dropdown_op = update_df()
hot_list.sort(key=lambda x: x[1])
data = {'Product-ID' : ['pid-'+str(id) for id, m in hot_list], "Quantity-Sold":[m for id, m in hot_list] }
dff = pd.DataFrame.from_dict(data)
# # dash Application
app = dash.Dash(__name__)

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H1("Sellometer", style={'text-align': 'center'}),
    dcc.Graph(id='live-ranks',animate=True),
    html.Br(),
    html.Br(),
    # dcc.Input(id="p_id",
    # type="number",
    # placeholder="Enter Product Id",
    # debounce=True,
    # value=hot_list[-1][0]
    # ),
    html.Br(),
    dcc.Graph(id='live-graph1', animate=True),
    dcc.Graph(id='live-graph2', animate=True),
    dcc.Graph(id='live-graph3', animate=True),
    dcc.Interval(
        id='graph-update',
        interval=60*1000,
        n_intervals=0
    ),
])
@app.callback([Output('live-ranks', 'figure'),
               Output('live-graph1', 'figure'),
               Output('live-graph2', 'figure'),
               Output('live-graph3', 'figure'),
              ],
              [Input('graph-update', 'n_intervals')
              ]
)
def update_graph_scatter(n):
    df_by_id, hot_list, dropdown_op = update_df()
    hot_list.sort(key=lambda x: x[1])
    data = {'Product-ID' : ['pid-'+str(id) for id, m in hot_list], "Quantity-Sold":[m for id, m in hot_list] }
    dff = pd.DataFrame.from_dict(data)
    # plot_df = df_by_id[str(p_id)]

    # Plotly Go
    trace1 = plotly.graph_objs.Scatter(
        x = df_by_id[str(hot_list[-1][0])]['event_time'],
        y = df_by_id[str(hot_list[-1][0])]['sum(price)'],
        name='Sales over time',
        # labels={'sum(price)': 'GMV ($)',
        # 'time_period':'Time'},
        mode='lines+markers'
    )

    trace2 = plotly.graph_objs.Scatter(
        x = df_by_id[str(hot_list[-2][0])]['event_time'],
        y = df_by_id[str(hot_list[-2][0])]['sum(price)'],
        name='Sales over time',
        # labels={'sum(price)': 'GMV ($)',
        # 'time_period':'Time'},
        mode='lines+markers'
    )

    trace3 = plotly.graph_objs.Scatter(
        x = df_by_id[str(hot_list[-3][0])]['event_time'],
        y = df_by_id[str(hot_list[-3][0])]['sum(price)'],
        name='Sales over time',
        # labels={'sum(price)': 'GMV ($)',
        # 'time_period':'Time'},
        mode='lines+markers'
    )

    barchart = px.bar(
        data_frame = dff,
        x = 'Product-ID',
        y = "Quantity-Sold",
        # orientation = "h",
        barmode='relative',
        labels={"product_id":"Product Id",
        "count(price)":"Quantity sold in past hour"},

    )

    return barchart, {'data': [trace1],
            'layout': go.Layout(
                xaxis=dict(range=[df_by_id[str(hot_list[-1][0])]['event_time'].min(), df_by_id[str(hot_list[-1][0])]['event_time'].max()]),
                yaxis=dict(range=[0, df_by_id[str(hot_list[-1][0])]['sum(price)'].max()*1.3]))
            }, {'data': [trace2],
                    'layout': go.Layout(
                        xaxis=dict(range=[df_by_id[str(hot_list[-2][0])]['event_time'].min(), df_by_id[str(hot_list[-2][0])]['event_time'].max()]),
                        yaxis=dict(range=[0, df_by_id[str(hot_list[-2][0])]['sum(price)'].max()*1.3]))
            }, {'data': [trace3],
                    'layout': go.Layout(
                        xaxis=dict(range=[df_by_id[str(hot_list[-3][0])]['event_time'].min(), df_by_id[str(hot_list[-3][0])]['event_time'].max()]),
                        yaxis=dict(range=[0, df_by_id[str(hot_list[-3][0])]['sum(price)'].max()*1.3]))
            }
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=False, port=8051, host="10.0.0.12")
