import pandas as pd
import plotly.express as px  # (version 4.7.0)
import plotly.graph_objects as go

import dash  # (version 1.12.0) pip install dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, Event
from collections import deque


import psycopg2
import os
from sqlalchemy import create_engine

def read_sql_to_df(engine, table_name, id_name = 'product_id'):
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

def update_from_sql():
    engine = create_engine(f"postgresql://{os.environ['psql_username']}:{os.environ['psql_pw']}@10.0.0.5:5431/ecommerce")

    # dataframe updates minute data
    df, df_gb = read_sql_to_df(engine, table_name="purchase_product_id_minute", id_name = 'product_id')

    # use hourly sales data to rank hot items
    df_hour, df_gb_hour = read_sql_to_df(engine, table_name="purchase_product_id_hour", id_name = 'product_id')
    hot_list = rank_by_id(df_gb_hour, rank_metric = "count(price)", n = 10)

    df_by_id, dropdown_op = id_time_series(hot_list, df, id_name = 'product_id')
    return df_by_id, dropdown_op, hot_list

df_by_id, dropdown_op, hot_list = update_from_sql()

# # dash Application
app = dash.Dash(__name__)

# ------------------------------------------------------------------------------
# App layout
app.layout = html.Div([

    html.H1("Sellometer", style={'text-align': 'center'}),

    dcc.Dropdown(id="slct_item",
                 options=dropdown_op,
                 multi=False,
                 value=hot_list[0][0],
                 style={'width': "40%"}
                 ),

    html.Div(id='output_container', children=[]),
    html.Br(),

    dcc.Graph(id='sale_timeseries', figure={})

])


# ------------------------------------------------------------------------------
# Connect the Plotly graphs with Dash Components
@app.callback(
    [Output(component_id='output_container', component_property='children'),
     Output(component_id='sale_timeseries', component_property='figure')],
    [Input(component_id='slct_item', component_property='value')]
)

def update_graph(option_slctd):
    print(option_slctd)
    df_by_id, dropdown_op, hot_list = update_from_sql()

    container = "The item id you selected was: {}".format(option_slctd)

    plot_df = df_by_id[option_slctd]
    # s = pd.to_numeric(dff['time_period'])
    # dff = dff.drop(columns=['time_period'])
    # dff = dff.merge(s.to_frame(), left_index=True, right_index=True)

    # Plotly Express
    fig = px.line(
        data_frame=plot_df,
        x = 'event_time',
        y = 'sum(price)',
        color = 'product_id',
        labels={'sum(price)': 'GMV ($)',
        'time_period':'Time'},
        template='plotly_dark'
    )

    return container, fig


# ------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=True, port=8051, host="10.0.0.12")
