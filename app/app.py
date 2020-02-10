import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input,Output
import dash_table
import pandas as pd
from sqlalchemy import create_engine
user='postgres'
password='postgres'
host='10.0.0.8'
port='5432'
db='postgres'
url='postgresql://{}:{}@{}:{}/{}'.format(user,password,host,port,db)

#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
con=create_engine(url)

app = dash.Dash(__name__)
server = app.server
#q="select renters.city_name from renters"
a="paris"
b="boston"
c="san-francisco"
q="select * from allcities"
df=pd.read_sql(q,con)

#def generate_table(dataframe, max_rows=11):
#    return html.Table(
#        # Header
#        [html.Tr([html.Th(col) for col in dataframe.columns])] +
#
#        # Body
#        [html.Tr([
#            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
#        ]) for i in range(min(len(dataframe), max_rows))]
#    )

#app = dash.Dash()
app.layout = html.Div(children=[
    html.H4(children='Popular listings'),
    dcc.Dropdown(id='dropdown', options=[
        {'label': i, 'value': i} for i in df.city_name.unique()
    ], placeholder='Filter by city...'),
    #html.Div(id='table-container')
    html.Div(children=[html.H1(children="Renter Summary",
                            style={
                                              'textAlign': 'center',
                                              "background": "yellow"})
                                  ]
                        ),
               html.Div(children=[html.Table(id='table'), html.Div(id='table-output')]),
])

@app.callback(Output('table-output', 'children'),
              [Input('dropdown', 'value')])
def get_data_table(option):
    #df = history.get_price(option, '20130428', '20200510')  # Get Bitcoin price data
    #df['date'] = pd.to_datetime(df['date'])
    df2 = df[df['city_name']==option]
    data_table = dash_table.DataTable(
        id='datatable-data',
        data=df2.to_dict('records'),
        columns=[{'id': c, 'name': c} for c in df2.columns],
        style_table={'overflowY': 'scroll'},
        fixed_rows={'headers': True, 'data': 10},
        style_cell={'width': '100px'},
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'
        },
        style_data_conditional=[{
        "if": {"row_index": 0},
        "backgroundColor": "#3D9970",
        'color': 'white'
    }]
    )
    return data_table


#@app.callback(
#    dash.dependencies.Output('table-container', 'children'),
#    [dash.dependencies.Input('dropdown', 'value')])
#def display_table(value):
    
    #dff = df[df.city_name.str.contains('|'.join(dropdown_value))]
    #where((col("city_name")==city))
    #print("Selected:",dropdown_value)
    #dff = df.where(where((col("city_name")==dropdown_value)))
#    dff=df[df[city_name]==value]
#    return dff.to_dict('records')

#app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True) 
