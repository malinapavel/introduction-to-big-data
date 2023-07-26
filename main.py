from pyspark.sql import SparkSession
from pyspark.sql.functions import col, ceil

import plotly.express as px
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output, ctx, State
import dash_bootstrap_components as dbc




spark = SparkSession.builder \
    .master("local[*]") \
    .appName('Erasmus Students Data') \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

### CSV -> PySpark DataFrame
df_spark = spark.read \
    .options(header=True, inferSchema=True, delimiter=',') \
    .csv('Erasmus.csv')


### Filtering and displaying Erasmus students data
def erasmus_data_filtering():
    df_student_cnt = df_spark.groupBy(["Receiving Country Code", "Sending Country Code"]) \
                             .count()
    df_student_cnt = df_student_cnt.orderBy("Receiving Country Code", "Sending Country Code")

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on every Receiving Country Code")
    #df_student_cnt.show(n=df_student_cnt.count())

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on a Receiving Country Code from the following: LV, MK, MT")
    df_filtered = df_student_cnt.where(col("Receiving Country Code").isin(["LV", "MK", "MT"]))
    #df_filtered.show(n=50)

    return df_student_cnt, df_filtered


### Storing data into the database
def erasmus_database(df, list_country_codes):
    all_countries_db()
    filtered_data_db(df)  # df stores the mobilities in LV, MK, MT
    list_countries_db(list_country_codes)


def all_countries_db():
    df_spark.write \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/erasmus_db") \
        .option("dbtable", "All_Countries") \
        .option("user", "root") \
        .option("password", "dummy") \
        .mode("overwrite") \
        .save()


# store data regarding mobilities in LV, MK, MT
def filtered_data_db(df):
      df.write \
        .format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/erasmus_db") \
        .option("dbtable", "Filtered_Countries") \
        .option("user", "root") \
        .option("password", "dummy") \
        .mode("overwrite") \
        .save()


# store data regarding mobilities from a list of given receiving codes
def list_countries_db(list_country_codes):
    for country in list_country_codes:
        table_name = country + "_Receiving"
        country_df = df_spark.filter(df_spark["Receiving Country Code"] == country).drop("Receiving Country Code")
        country_df.write \
            .format("jdbc") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("url", "jdbc:mysql://localhost:3306/erasmus_db") \
            .option("dbtable", table_name) \
            .option("user", "root") \
            .option("password", "dummy") \
            .mode("overwrite") \
            .save()


# df_filter_all -> all mobilities grouped by receiving and sending codes
# df_filter -> only the mobilities from LV, MK, MT as receiving country codes
df_filtered_all, df_filtered = erasmus_data_filtering()
#list_country_codes = ['RO', 'HR', 'IT']
#erasmus_database(df_filtered, list_country_codes)





### Preparing DataFrames suitable for average age and mobility duration
df_avg_age = df_spark.groupBy('Sending Country Code').mean()
df_avg_age = df_avg_age.drop('avg(Mobility Duration)')\
                       .withColumnRenamed('avg(Participant Age)', 'Average Participant Age')\
                       .toPandas()
#print(df_avg_age.head(100))

df_avg_mobility = df_spark.groupBy('Receiving Country Code').mean()
df_avg_mobility = df_avg_mobility.drop('avg(Participant Age)')\
                                 .withColumnRenamed('avg(Mobility Duration)', 'Average Mobility Duration')\
                                 .toPandas()
#print(df_avg_mobility.head(100))

EU_countries = ['Albania', 'Austria', 'Belgium', 'Belarus', 'Bosnia', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia', 'Denmark',
                'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kosovo',
                'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Moldova', 'Montenegro', 'Netherlands','Norway', 'North Macedonia',
                'Poland', 'Portugal', 'Romania', 'Russia', 'Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'Switzerland',
                'Turkey', 'UK', 'Ukraine']




### Application development for data visualization

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container(children=[
    html.H1(children="Erasmus mobility data visualization",\
            style={'textAlign': 'center',
                   'marginTop': 50}),
    dbc.Container(className="col-lg-10 offset-lg-2",
                  style={'marginTop': 100},
                  children=[
                    dbc.Button(children="Average Age/Country", id="btn1", color="primary", className="me-5"),
                    dbc.Button(children="Average Mobility Duration/Country", id="btn2", color="warning", className="me-5"),
                    dbc.Button(children="Students Sent/Country", id="btn3", color="success", className="me-5")
                    # dbc.Button("Button 4", id="btn4", color="warning", className="me-5"),
                    # dbc.Button("Button 5", id="btn5", color="info", className="me-5"),
                    # dbc.Button("Button 6", id="btn6", color="danger", className="me-5")
    ]),
    dbc.Container(id="enter_field", style={'marginTop': 70, 'display': 'none'},
                  children=[dbc.FormFloating(
                                style={'marginTop': 70},
                                className="col-lg-3",
                                children=[
                                    dbc.Input(id="country_code"),
                                    dbc.Label("Enter a country code"),
                                ]),
                             dbc.Button("Visualize data", id="btn4", color="secondary", className="me-5", style={'marginTop': 30})]),
    dbc.Container(style={'marginTop': 70, 'marginLeft': 100},
                  children=[
                    html.Div(id="output_container")]),
    dbc.Container(id="display_map", style={'marginTop': 10, 'display': 'none'},
                  children=[dcc.Graph(id='country_map')])
], fluid=True)

@app.callback(
    Output("enter_field", "style"),
    Output("output_container", "children"),
    Output("display_map", "style"),
    Output("country_map", "figure"),
    Input("btn1", "n_clicks"),
    Input("btn2", "n_clicks"),
    Input("btn3", "n_clicks"),
    Input("btn4", "n_clicks"),
    State("country_code", "value"),
    prevent_initial_call=True
)
def greet(_, __, ___, ____, code):
    button_clicked = ctx.triggered_id

    match button_clicked:
        case 'btn1':
            text = html.H6(children="Average age of students attending Erasmus mobilities for each country they come from [sending countries]:",\
                           style={'marginTop': 50})
            fig = go.Figure(data=go.Choropleth(
                            locations=EU_countries,
                            z=df_avg_age['Average Participant Age'].astype(int),
                            locationmode='country names'
                  ))
            fig.update_layout(height=800, geo_scope="europe")
            return {'display': 'none'}, text, {'display': 'block'}, fig

        case 'btn2':
            text = html.H6(children="Average mobility duration performed by students for each country [receiving countries]:",\
                           style={'marginTop': 50})
            fig = go.Figure(data=go.Choropleth(
                            locations=EU_countries,
                            z=df_avg_mobility['Average Mobility Duration'].astype(int),
                            locationmode='country names'
                  ))
            fig.update_layout(height=800, geo_scope="europe")
            return {'display': 'none'}, text, {'display': 'block'}, fig

        case 'btn3':
            text = html.H6(children="Number of students that went to a specific host country:",
                           style={'marginTop': 50})

            df_cnt = df_filtered_all.where(col("Receiving Country Code") == code).toPandas()
            fig = go.Figure(data=go.Choropleth(
                    locations=EU_countries,
                    z=df_cnt['count'].astype(int),
                    locationmode='country names'
            ))
            fig.update_layout(height=800, geo_scope="europe")
            return {'display': 'block'}, text, {'display': 'block'}, fig



app.run_server(debug=True)