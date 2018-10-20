import traceback

import pandas as pd

from util.util import load_data_from_db, get_hamming_score, get_raw_data_dict, get_matched_data_dict, \
    get_potential_matched_url
from connection_manager import oracle_connection_manager as cm


def url_matcher():
    try:
        # Initialize data frames
        csv_log_file_df, matched_data_df, raw_data_df = initialize_data_frames()

        # Iterate over csv file to find matches
        for row in csv_log_file_df.itertuples():
            # If both matched_data_df and raw_data_df are blank means the System is new
            # So we just insert the first fow of csv_log_file_df into raw_data_df
            if len(matched_data_df) == 0 and len(raw_data_df) == 0:
                # Create a dictionary
                # Convert dictionary to a data frame
                # Append one dataFrame to another
                token_string = ','.join(row.tokens)
                raw_data_dict = get_raw_data_dict(row, token_string)
                data_dict_df = pd.DataFrame(raw_data_dict, index=[id])
                raw_data_df = raw_data_df.append(data_dict_df, sort=True)

            # If matched_data_df is blank but raw_data_df has data
            # We can find a potential match
            elif len(matched_data_df) == 0:
                matched_data_df, raw_data_df = check_raw_data_match(matched_data_df, raw_data_df, row)
            else:
                # If we have both matched_data_df and raw_data_df populated
                # Which will be the usual case
                # Then we try to find a match in matched_data_df first
                # If no match in matched_data_df then
                # find a match in raw_data_df
                print(row.token_count)
                token_count_int = int(matched_data_df["token_count"])
                print(token_count_int)
                matched_data_df_filtered_on_token_count = matched_data_df[
                    matched_data_df["token_count"] == row.token_count]
                match_found_flag = False
                for index, j in enumerate(matched_data_df_filtered_on_token_count.itertuples(), 0):
                    if j.token_position == "":
                        # TODO Change data frame column names
                        if j["potential_matched_url"] == row.potential_matched_url:
                            # Since we already have a matched URL
                            # Nothing else to do
                            # TODO Add a colum to increase match count
                            match_found_flag = True
                            break
                        else:
                            continue
                    else:
                        potential_row_url = get_potential_matched_url(row.tokens, j.token_position)
                        if potential_row_url == matched_data_df_filtered_on_token_count[
                            "potential_matched_url"][index]:
                            # Since we already have a matched URL
                            # Nothing else to do
                            # TODO Add a colum to increase match count
                            match_found_flag = True
                            break
                        else:
                            continue
                if not match_found_flag:
                    print(22222222222222)
                    check_raw_data_match(matched_data_df, raw_data_df, row)
                                         #matched_data_df, raw_data_df
        print(type(raw_data_df["modified_flag"] == True))
        raw_data_df_to_persist = raw_data_df[raw_data_df["modified_flag"] == True]#.reset_index()
        matched_data_df_to_persist = matched_data_df[matched_data_df["modified_flag"] == True]#.reset_index()
        matched_data_df_to_persist["auto_matched"] = "N"
        raw_data_df_to_persist = raw_data_df_to_persist.drop(["modified_flag"],axis=1,)
        matched_data_df_to_persist = matched_data_df_to_persist.drop(["modified_flag"],axis=1)
        persist_df(raw_data_df_to_persist, "raw_data")
        persist_df(matched_data_df_to_persist, "matched_data")
    except Exception as e:
        traceback.print_exc(e)


def persist_df(dataframe, table_name):
    connection = cm.ManageConnection().get_connection()
    dataframe.to_sql(table_name, connection, if_exists="append", index=False)


def initialize_data_frames():
    # Initialize data frames and add required columns
    raw_data_df = load_data_from_db("select * from raw_data")
    matched_data_df = load_data_from_db("select * from matched_data")
    csv_log_file_df = pd.read_csv("../log_of_urls_invoked.csv")
    csv_log_file_df["tokens"] = csv_log_file_df["URL"].str.strip("'").str.strip('/').str.split('/')
    csv_log_file_df["token_count"] = csv_log_file_df["tokens"].str.len()
    raw_data_df["modified_flag"] = None
    matched_data_df["modified_flag"] = None
    return csv_log_file_df, matched_data_df, raw_data_df


def check_raw_data_match(matched_data_df, raw_data_df, row):
    # Filter raw_data_df using token_count as criteria
    # Should be a big performance boost in long run
    df_filtered_on_token_count = raw_data_df[(raw_data_df.token_count == row.token_count)]
    # Loop through df_filtered_on_token_count to find a potential match
    # Get Hamming Score
    row_count_in_filtered_df = len(df_filtered_on_token_count.index)
    for index, raw_data_row in enumerate(df_filtered_on_token_count.itertuples(), 1):
        hamming_score, mismatch_token_index_str = get_hamming_score(raw_data_row.tokens.split(','),
                                                                    row.tokens)
        token_string = ','.join(row.tokens)
        # If hamming Score = 0 then Current row matches exactly with a row in raw_data_df
        # In this case we just increase the hit count
        if hamming_score == 0:
            # Get raw_data_df row to update
            # Update hit_count and mark modified_flag = True
            # So we can filter on flag to upsert
            raw_data_df_row = raw_data_df.loc[raw_data_df["id"] == raw_data_row.id]
            raw_data_df.loc[raw_data_df["id"] == raw_data_row.id, ["hit_count", "modified_flag"]] = \
                raw_data_df_row['hit_count'].iloc[0] + 1, True
            break
        # If hamming Score = 1 then
        # We Found a potential match
        # We add row into matched_data_df
        # We update raw_data_df foreign key
        elif hamming_score == 1:
            potential_matched_url = get_potential_matched_url(row.tokens, mismatch_token_index_str)
            matched_data_dict = get_matched_data_dict(hamming_score, mismatch_token_index_str, potential_matched_url,
                                                      row, token_string)
            matched_data_dict_to_df = pd.DataFrame(matched_data_dict, index=["id"])
            matched_data_df = matched_data_df.append(matched_data_dict_to_df, sort=True)
            break
        else:
            # If hamming_score > 1
            # Not handling such cases now
            # But we may want to support in future
            # So a TODO
            # For now in such case, it is just another raw_data with no match
            # so just insert in raw_data_df
            # When we are in the last iteration
            if row_count_in_filtered_df == index:
                raw_data_dict = get_raw_data_dict(row, token_string)
                data_dict_df = pd.DataFrame(raw_data_dict, index=[id])
                raw_data_df = raw_data_df.append(data_dict_df, sort=True)
    return matched_data_df, raw_data_df


url_matcher()
