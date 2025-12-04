import datetime
import swifter
import plotly.graph_objects as go
import plotly.io as pio
import plotly.offline as pyo
from config import DBSCAN_PARAMETERS_DICT


class ClusterVisualizer:
    """Cluster coverage visualization for air compressors."""

    def __init__(self, air_compressor_meta_df, run_time_df, malfunction_df):
        pyo.init_notebook_mode()
        pio.renderers.default = 'browser'  # Set default plotly renderers to browser.

        self.air_compressor_meta_df = air_compressor_meta_df
        self.run_time_df = run_time_df
        self.dbscan_parameters_dict = DBSCAN_PARAMETERS_DICT

        malfunction_df = malfunction_df[['asset_code', 'report_date', 'action_complete']]
        malfunction_df.drop_duplicates(inplace=True)
        malfunction_df.rename(columns={'action_complete': 'finish_date'}, inplace=True)

        malfunction_df['asset_code'] = malfunction_df['asset_code'].astype(str)  # Set proper data types.
        malfunction_df['report_date'] = malfunction_df['report_date'].swifter.progress_bar(False).apply(pd.Timestamp)
        malfunction_df['finish_date'] = malfunction_df['finish_date'].swifter.progress_bar(False).apply(pd.Timestamp)

        # Only keep malfunction records since 2020.
        malfunction_df = malfunction_df[(malfunction_df['report_date'] >= datetime.datetime(2020, 1, 1, 0, 0, 0))
                                        | (malfunction_df['finish_date'] >= datetime.datetime(2020, 1, 1, 0, 0, 0))]
        # Store malfunction data into property in time-ascending order.
        malfunction_df.sort_values(by=['report_date'], ascending=True, inplace=True)
        malfunction_df.reset_index(drop=True, inplace=True)
        self.malfunction_df = malfunction_df

    def plot(self):
        meta_df = self.air_compressor_meta_df
        run_time_df = self.run_time_df
        malfunction_df = self.malfunction_df

        for asset_code in sorted(set(run_time_df['asset_code'])):
            run_time_df_iter = run_time_df[run_time_df['asset_code'] == asset_code]
            run_time_df_iter.set_index('datetime', inplace=True)

            mal_df_iter = malfunction_df[malfunction_df['asset_code'] == asset_code]
            location_iter = meta_df.loc[meta_df['asset_code'] == asset_code, 'location'].iloc[0]

            minimal_samples_iter = self.dbscan_parameters_dict[location_iter]['minimal_samples']
            epsilon_iter = self.dbscan_parameters_dict[location_iter]['epsilon']

            fig = go.Figure(layout=go.Layout(
                updatemenus=[dict(type='buttons', direction='right', x=0.9, y=1.16)],
                xaxis=dict(range=[run_time_df_iter.index.min() - datetime.timedelta(days=1),
                                  run_time_df_iter.index.max() + datetime.timedelta(days=1)],
                           autorange=False, tickwidth=2, title_text='Time'),
                yaxis=dict(
                    range=[run_time_df_iter['runtime'].min() * 0.8,
                           run_time_df_iter['runtime'].max() * 1.2],
                    autorange=False, title_text='Runtime (minutes)')
            ))

            layout_title = f'Post-2020 {location_iter} Air Compressor {asset_code} DBSCAN Coverage<br>'
            layout_title += f'Minimal Samples = {str(minimal_samples_iter)}, Epsilon = {str(epsilon_iter)}'
            fig.update_layout(title_text=layout_title, title_x=0.1, title_y=0.95)

            # Plot normal run time in green.
            fig.add_trace(go.Scatter(x=run_time_df_iter.index,
                                     y=run_time_df_iter['runtime'].where(run_time_df_iter['cluster'] == 1),
                                     name='Normal Run Time',
                                     mode='lines', marker=dict(color='green'), showlegend=True))

            # Plot deviated run time in yellow.
            fig.add_trace(go.Scatter(x=run_time_df_iter.index,
                                     y=run_time_df_iter['runtime'].where(run_time_df_iter['cluster'] != 1),
                                     name='Uncommonly Deviated Run Time',
                                     mode='markers', marker=dict(color='yellow'), showlegend=True))

            # Plot last 7-day average run time in sky blue.
            fig.add_trace(go.Scatter(x=run_time_df_iter.index,
                                     y=run_time_df_iter['last_7_day_avg'],
                                     name='Last 7-Day Avg',
                                     mode='lines', marker=dict(color='skyblue'), showlegend=True))

            # Add red annotations for reported malfunctions since 2020.
            if len(mal_df_iter) > 0:
                for i in range(len(mal_df_iter.index)):
                    # Set begin date as the earlier one between report date and finish date.
                    begin_date = min(mal_df_iter['report_date'].iloc[i], mal_df_iter['finish_date'].iloc[i])
                    # Set begin date into hourly level.
                    begin_date = datetime.datetime(begin_date.year, begin_date.month, begin_date.day,
                                                   begin_date.hour, 0, 0)

                    if begin_date >= min(list(run_time_df_iter.index)):
                        run_time_begin = run_time_df_iter.loc[run_time_df_iter.index == begin_date, 'runtime'][0]

                        fig.add_annotation(x=begin_date, y=run_time_begin, xref='x', yref='y',
                                           text=''.join(['Mal ', str(i + 1), ' Start: ', str(begin_date.year), '/',
                                                         str(int(begin_date.month)), '/', str(begin_date.day)]),
                                           showarrow=True,
                                           font=dict(
                                               family='Courier New, monospace', size=16, color='white'
                                           ),
                                           align='center', arrowhead=2, arrowsize=1, arrowwidth=2, arrowcolor='white',
                                           ax=20, ay=-30, bordercolor='white', borderwidth=2, borderpad=4,
                                           bgcolor='red', opacity=0.8
                                           )

            fig.update_layout(xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(label='1 Month', count=1, step='month', stepmode='backward'),
                        dict(label='3 Months', count=3, step='month', stepmode='backward'),
                        dict(label='6 Months', count=6, step='month', stepmode='backward'),
                        dict(label='1 Year', count=1, step='year', stepmode='backward'),
                        dict(label='1 YTD', count=1, step='year', stepmode='todate'),
                        dict(label='All', step='all')
                    ]),
                ),

                rangeslider=dict(visible=True),
                type='date'
            )
            )

            fig.update_layout(template='plotly_dark',
                              xaxis_rangeselector_bgcolor='green',
                              xaxis_rangeselector_font_color='white',
                              xaxis_rangeselector_activecolor='red'
                              )

            fig.update_xaxes(ticks='outside', tickwidth=2, tickcolor='white', ticklen=10, showgrid=False)
            fig.update_yaxes(ticks='outside', tickwidth=2, tickcolor='white', ticklen=1, showgrid=False)
            fig.update_layout(yaxis=dict(fixedrange=False), yaxis_tickformat=',')
            fig.update_layout(legend=dict(x=0, y=1.1), legend_orientation='h')
            fig.show()


if __name__ == '__main__':
    import os
    import pandas as pd
    from config import APP_BASE_PATH
    from dao import air_compressor_meta_dao, runtime_dao
    from workflow.moving_avg_calculation_step import CalculationStep
    from algorithm.dbscan_test import DBSCANTester

    runtime_data_main = runtime_dao.read_runtime()  # Runtime data.

    if len(runtime_data_main) > 0:  # If runtime data exist.
        # Filter meta and runtime data by list of targeted asset codes.
        targeted_asset_codes_list = ['81000946', '10100007', '10100024', '10100031']

        meta_data_main = air_compressor_meta_dao.read_air_compressor_meta()  # Air compressor meta data.
        meta_data_main = meta_data_main[meta_data_main['asset_code'].isin(targeted_asset_codes_list)]

        runtime_data_main = runtime_data_main[runtime_data_main['asset_code'].isin(targeted_asset_codes_list)]
        # Only use data since 2020.
        datetime_threshold = datetime.datetime(2020, 1, 1, 0, 0, 0)
        runtime_data_main = runtime_data_main[runtime_data_main['datetime'] >= datetime_threshold]

        calculation_step = CalculationStep()
        calculation_step.conduct(runtime_data_main)
        runtime_data_main = calculation_step.runtime_df

        tester = DBSCANTester(runtime_data_main)  # DBSCAN Clustering.
        tester.evaluate(meta_data_main, None, False, False)

        if len(tester.abnormal_cluster_df) > 0:
            runtime_data_main = tester.runtime_df
            malfunction_data_main = pd.read_csv(os.path.join(APP_BASE_PATH, 'test', 'malfunction_records.csv'))
            visualizer = ClusterVisualizer(meta_data_main, runtime_data_main, malfunction_data_main)
            visualizer.plot()
