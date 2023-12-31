from pyspark.sql.functions import col

import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, ctx, State
import dash_bootstrap_components as dbc
import data_processing as dp



# Country list for the 'locations' field in go.Choropleth
EU_countries = ['Albania', 'Austria', 'Belgium', 'Belarus', 'Bosnia', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark',
                'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kosovo',
                'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Montenegro', 'Netherlands','Norway', 'North Macedonia',
                'Poland', 'Portugal', 'Romania', 'Russia', 'Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland',
                'Turkey', 'UK', 'Ukraine']
EU_countries_2 = ['United Kingdom', 'Switzerland', 'Spain', 'Sweden', 'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Czechia', 'Denmark',
                'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary','Ireland', 'Italy',
                'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Montenegro', 'Netherlands','Norway', 'North Macedonia',
                'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia']
EU_countries_3 = ['Hungary', 'Italy', 'Spain', 'Latvia', 'Slovakia', 'Austria', 'Czechia', 'Croatia', 'Belgium', 'France', 'Bulgaria', 'United Kingdom',  'Switzerland',
                  'Romania',  'Sweden', 'Denmark', 'Estonia', 'Finland',   'Greece', 'Ireland',
                  'Lithuania', 'Luxembourg', 'Malta', 'Montenegro', 'Netherlands','Norway', 'North Macedonia',
                  'Poland', 'Portugal',  'Slovenia']




def render_app():
    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

    app.layout = dbc.Container(children=[
        html.H1(children="Erasmus mobility data visualization",\
                style={'textAlign': 'center',
                       'marginTop': 50}),
        dbc.Container(className="h-100 d-flex align-items-center justify-content-center",
                      style={'marginTop': 100},
                      children=[
                        dbc.Button(children="Average Age/Country", id="btn1", color="primary", className="me-5"),
                        dbc.Button(children="Average Mobility Duration/Country", id="btn2", color="warning", className="me-5"),
                        dbc.Button(children="Students Sent/Country", id="btn3", color="success", className="me-5")
        ]),
        dbc.Container(style={'marginTop': 70, 'marginLeft': 100},
                      children=[
                        html.Div(id="output_container")]),
        dbc.Container(id="enter_field", style={'marginTop': 70, 'display': 'none'},
                      className="col-lg-3",
                      children=[
                          dbc.FormFloating(style={'marginTop': 70},
                                           children=[
                                                dbc.Input(id="country_code"),
                                                dbc.Label("Enter a country code"),
                                           ]),
                          dbc.Button("Visualize data", id="btn_sub", color="secondary", className="me-5",
                                     style={'marginTop': 30})]),
        dbc.Container(id="display_map", style={'marginTop': 10, 'display': 'none'},
                      children=[dcc.Graph(id='country_map')])
    ], fluid=True)

    @app.callback(
        Output("output_container", "children"),
        Output("enter_field", "style"),
        Output("display_map", "style"),
        Output("country_map", "figure"),
        Input("btn1", "n_clicks"),
        Input("btn2", "n_clicks"),
        Input("btn3", "n_clicks"),
        Input("btn_sub", "n_clicks"),
        State("country_code", "value"),
        prevent_initial_call=True
    )
    def greet(_, __, btn3_click, ____, code):
        button_clicked = ctx.triggered_id

        if button_clicked == 'btn1':
                df_avg_age = dp.average_age()

                text = html.H6(children="Average age of students attending Erasmus mobilities for each country they come from [sending countries]:",\
                               style={'marginTop': 50})
                fig = go.Figure(data=go.Choropleth(
                                locations=EU_countries,
                                z=df_avg_age['Average Participant Age'].astype(int),
                                locationmode='country names'
                      ))
                fig.update_layout(height=800, geo_scope="europe")
                return text, {'display': 'none'}, {'display': 'block'}, fig

        elif button_clicked == 'btn2':
                df_avg_mobility = dp.average_mobility_duration()

                text = html.H6(children="Average mobility duration performed by students for each country [receiving countries]:",\
                               style={'marginTop': 50})
                fig = go.Figure(data=go.Choropleth(
                                locations=EU_countries,
                                z=df_avg_mobility['Average Mobility Duration'].astype(int),
                                locationmode='country names'
                      ))
                fig.update_layout(height=800, geo_scope="europe")
                return text, {'display': 'none'}, {'display': 'block'}, fig

        elif button_clicked == 'btn3' or (button_clicked == 'btn_sub' and btn3_click is not None):
                df_filtered_all = dp.erasmus_data_filtering()[0]

                text = html.H6(children="Number of students that went to a specific host country:",
                               style={'marginTop': 50})

                df_cnt = df_filtered_all.where(col("Receiving Country Code") == code)\
                                        .drop("Receiving Country Code")\
                                        .toPandas()
                fig = go.Figure(data=go.Choropleth(
                                locations=EU_countries_3,
                                z=df_cnt['count'].astype(int),
                                locationmode='country names'
                ))
                fig.update_layout(height=800, geo_scope="europe")
                return text, {'display': 'block'}, {'display': 'block'}, fig


    return app

