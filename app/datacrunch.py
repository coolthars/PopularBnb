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
#con=create_engine(url)

###### COpying 
app = dash.Dash(__name__)
server = app.server
#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
con=create_engine(url)
q="select * from allcities"
df=pd.read_sql(q,con)
inv="select * from invtest"
df_inv=pd.read_sql(inv,con)
print(len(df_inv))

Rexternal_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

#app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Since we're adding callbacks to elements that don't exist in the app.layout,
# Dash will raise an exception to warn us that we might be
# doing something wrong.
# In this case, we're adding the elements through a callback, so we can ignore
# the exception.
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content')
])


index_page = html.Div([
    dcc.Link('Go to Renters Insights Page', href='/page-1'),
    html.Br(),
    dcc.Link('Go to Investors Insights Page', href='/page-2'),
])

page_1_layout = html.Div([
    html.H1('Renters Insights'),
    dcc.Dropdown(id='dropdown', options=[
        {'label': i, 'value': i} for i in df.city_name.unique()
    ], placeholder='Filter by city...'),
    #html.Div(id='page-1-content')
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
               html.Div(children=[html.Table(id='table'), html.Div(id='table-output')]),
    html.Br(),
    dcc.Link('Go to Investors Insights', href='/page-2'),
    html.Br(),
    dcc.Link('Go back to home', href='/'),
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
#### End Copy

#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

#app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Since we're adding callbacks to elements that don't exist in the app.layout,
# Dash will raise an exception to warn us that we might be
# doing something wrong.
# In this case, we're adding the elements through a callback, so we can ignore
# the exception.

page_2_layout=html.Div([
    html.H1('Investors Insights'),
    dcc.Dropdown(id='dropdown', options=[
        {'label': i, 'value': i} for i in df_inv.city_name.unique()
    ], placeholder='Filter by city...'),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),
               html.Div(children=[html.Table(id='table'), html.Div(id='table-output2')]),
    html.Br(),
    dcc.Link('Go to Renters Insights', href='/page-1'),
    html.Br(),
    dcc.Link('Go back to home', href='/'),
])
@app.callback(Output('table-output2', 'children'),
              [Input('dropdown', 'value')])
def get_data_table(optioninv):
    #df = history.get_price(option, '20130428', '20200510')  # Get Bitcoin price data
    #df['date'] = pd.to_datetime(df['date'])
    df_inv2 = df_inv[df_inv['city_name']==optioninv]
    data_table2 = dash_table.DataTable(
        id='datatable-data',
        data=df_inv2.to_dict('records'),
        columns=[{'id': c, 'name': c} for c in df_inv2.columns],
        style_table={'overflowY': 'scroll'},
        fixed_rows={'headers': True, 'data': 10},
        style_cell={'width': '100px'},
        style_header={
            'backgroundColor': 'rgb(230, 230, 230)',
            'fontWeight': 'bold'
        },
        style_data_conditional=[{
        "if": {"row_index": 0},
        "backgroundColor": "#ff5a5f",
        'color': 'black'
    }]
    )
    return data_table2
#####
# Update the index
@app.callback(dash.dependencies.Output('page-content', 'children'),
              [dash.dependencies.Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/page-1':
        return page_1_layout
    elif pathname == '/page-2':
        return page_2_layout
    else:
        return index_page
    # You could also return a 404 "URL not found" page here
#q="select renters.city_name from renters"
#    return dff.to_dict('records')

#app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
if __name__ == '__main__':
    app.run_server(host='0.0.0.0',debug=True) 
