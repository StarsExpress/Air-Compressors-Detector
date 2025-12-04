
def calculate_last_7_day_avg(runtime_df, logger=None):  # Calculate last 7-day average run time.
    runtime_df.set_index('datetime', inplace=True)
    runtime_df['last_7_day_avg'] = runtime_df.groupby('asset_code')['runtime'].rolling(168).mean().values
    runtime_df.insert(0, 'datetime', runtime_df.index)  # Put datetime back at first column.

    runtime_df.dropna(inplace=True)
    runtime_df.reset_index(drop=True, inplace=True)
    runtime_df['last_7_day_avg'] = runtime_df['last_7_day_avg'].round(0).astype(int)  # Set last 7-day avg as int.
    if logger is not None:
        logger.write('debug', 'Last 7-day average calculations completed.')

    return runtime_df
