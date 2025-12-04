import pandas as pd
import swifter


def clean_and_interpolate(input_df, tag_code_dict):
    # Melt input data frame into proper format.
    melted_df = pd.melt(input_df, id_vars=['datetime'], value_vars=list(input_df.columns)[1:],
                        var_name='asset_code', value_name='runtime')
    # Map tag names into asset codes.
    melted_df['asset_code'] = melted_df['asset_code'].swifter.progress_bar(False).apply(lambda x: tag_code_dict[x])

    # Every unique tuple of timestamp and asset code can only have one record.
    melted_df = melted_df[~melted_df.duplicated(subset=['datetime', 'asset_code'], keep='first')]
    # Sort by ascending datetime and then asset code.
    melted_df.sort_values(by=['datetime', 'asset_code'], ascending=[True, True], inplace=True)

    melted_df['runtime'] = pd.to_numeric(melted_df['runtime'], errors='coerce')
    melted_df['runtime'].fillna(0, inplace=True)  # Fill NaN by 0 and set integer.
    melted_df['runtime'] = melted_df['runtime'].astype(int)

    melted_df.reset_index(drop=True, inplace=True)
    return melted_df
