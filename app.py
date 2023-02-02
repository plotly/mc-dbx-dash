import dash_ag_grid as dag
import dash_design_kit as ddk
from theme import theme
import dash
from dash import Input, Output, State, html, dcc, dash_table
import pandas as pd
import json
import urllib

import plotly.express as px
from dbx_utils import df, engine 

app = dash.Dash(__name__)
server = app.server
columnDefs = [{"headerName": x, "field": x,
                 'filter':True,
                 'floatingFilter':True,
                 'filterParams': {'buttons': ['apply', 'reset']}} for x in df.columns]
rowData = df.to_dict('records')
app.title = "Molson Coors - Production Data App (Demo)"

app.layout = ddk.App(
    children=[html.Div(
    [
        ddk.Header(
            children=[
                ddk.Title(app.title)
            ]
        ),
        ddk.Card(
            dag.AgGrid(
                id='downloadable-grid',
                enableEnterpriseModules=True,
                columnDefs=columnDefs,
                rowData= rowData,
                #theme='alpine',
                enableCellTextSelection=True,
                suppressCopyRowsToClipboard=True,
                suppressRowClickSelection=True,
                enableRangeSelection=True,
                rowSelection='multiple',
                pagination=True,
                paginationPageSize=50,
                style={'height':'550px'},
                enableBrowserTooltips=True,
                suppressMovableColumns=True,
                enableCellChangeFlash=True,
                # columnSize='autoSizeAll', # 'autoSizeAll' slows things down significantly
                defaultColDef=dict(
                    resizable=True,
                    editable=True,
                    sortable=True,
                    autoHeight=True,
                    width=150
                ),
                sideBar=True,
            ),
        ),
        html.Div(
            html.Button(
                [
                    html.A(
                        "Download CSV",
                        id="download-link",
                        download="datatable.csv",
                        href="",
                        target="_blank",
                        style={
                            "text-decoration": "none",
                            "color": "white",
                        },
                    )
                ],
                id="csv-button",
            ),
            style={"margin": "20px"},
        ),
        html.Div(
            html.Button(
                [
                    html.A(
                        "Save to Databricks",
                        #id="download-link",
                        #download="datatable.csv",
                        href="",
                        target="_blank",
                        style={
                            "text-decoration": "none",
                            "color": "white",
                        },
                    )
                ],
                id="db-button",
            ),
            style={"margin": "20px"},
        ),
    ]
),
    ], theme=theme, show_editor=True,
    )


@app.callback(
    Output("download-link", "href"),
    Input("downloadable-grid", "rowData"),
)
def dl_csv(rowData):
    if rowData is None:
        return dash.no_update
    else:
        df = pd.DataFrame(rowData)
        df.to_sql('mc_aggrid_demo.bronze_sensors', con=engine, index=False, method="multi", if_exists= 'append')
        csv_string = df.to_csv(index=False, encoding="utf-8")
        csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string)
        return csv_string


if __name__ == "__main__":
    app.run_server(debug=True)