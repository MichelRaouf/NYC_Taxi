import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

def histogram(df, column):
    fig = px.histogram(df, x=column)
    return fig

def create_bar_chart(dataframe, xlabel='', ylabel=''):
    fig = px.bar(dataframe, x=dataframe.index, y=dataframe.values, color=dataframe.index)
    fig.update_layout(xaxis_title=xlabel, yaxis_title=ylabel)
    return fig

def create_line_chart(dataframe, xlabel='', ylabel=''):
    fig = px.line(dataframe, x=dataframe.index, y=dataframe.values, markers=True)
    fig.update_layout(xaxis_title=xlabel, yaxis_title=ylabel)
    return fig

def initialize_dashboards(filename):
    nyc_df = pd.read_csv(filename)
    fare_amount_by_payment_type = nyc_df.groupby('payment_type')['fare_amount'].mean()
    distance_by_passenger_count = nyc_df.groupby('passenger_count')['trip_distance'].mean()
    trip_distance_bt_trip_type = nyc_df.groupby('trip_type')['trip_distance'].mean()
    app = Dash()
    app.layout = html.Div([
        html.H1("Michel Raouf Ramzy Iskander", style={'text-align': 'center'}),
        html.H1("49-0831", style={'text-align': 'center'}),
        html.H1("MET", style={'text-align': 'center'}),
        html.H2("Trip Distance Distribution Histogram", style={'text-align': 'center'}),
        dcc.Graph(figure=histogram(nyc_df, "trip_distance")),
        html.H2("Fare Amount Across Payment Types", style={'text-align': 'center'}),
        dcc.Graph(figure=create_bar_chart(fare_amount_by_payment_type, xlabel='Payment Type', ylabel='Fare Amount')),
        html.H2("Trip Distance Across Passenger Count'", style={'text-align': 'center'}),
        dcc.Graph(figure=create_line_chart(distance_by_passenger_count, xlabel='Passenger Count', ylabel='Trip Distance')),
        html.H2("Trip Distance Across Trip Type", style={'text-align': 'center'}),
        dcc.Graph(figure=create_bar_chart(trip_distance_bt_trip_type, xlabel='Trip Type', ylabel='Trip Distance')),
    ])

    app.run_server(host='0.0.0.0', debug=False)