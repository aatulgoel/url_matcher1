import traceback

import pandas as pd

from connection_manager import oracle_connection_manager as cm
from util.util import load_data_from_db, get_hamming_score, get_raw_data_dict, get_matched_data_dict, \
    get_potential_matched_url


def url_matcher():
    try:
        # Initialize data frames
        csv_log_file_df, matched_data_df, raw_data_df = initialize_data_frames()

        counter = 0
        for row in csv_log_file_df.itertuples():
            counter = counter + 1
            print("counter = ", counter)
            # If both matched_data_df and raw_data_df are blank means the system is new
            # So we just insert the first fow of csv_log_file_df into raw_data_df
            if len(matched_data_df.index) == 0 and len(raw_data_df.index) == 0:
                raw_data_df = append_row_to_raw_df(raw_data_df, row, generate_csv_string_from_list(row.tokens))

            # If matched_data_df is blank but raw_data_df has data
            # We can find a potential match
            elif len(matched_data_df) == 0:
                raw_data_df, matched_data_df = handle_no_existing_matched_url_scenario(matched_data_df, raw_data_df,
                                                                                       row)
            else:
                # If we have both matched_data_df and raw_data_df populated
                # Which will be the usual case
                # Then we try to find a match in matched_data_df first
                # If no match in matched_data_df then
                # find a match in raw_data_df
                existing_match_found, matched_row_id = find_if_url_is_already_matched(matched_data_df, row)

                if existing_match_found:
                    matched_data_df = update_hit_count_value(matched_data_df, matched_row_id)
                else:
                    raw_data_df, matched_data_df = handle_no_existing_matched_url_scenario(matched_data_df, raw_data_df,
                                                                                           row)

        raw_data_df_to_persist = raw_data_df[raw_data_df["modified_flag"] == True]
        matched_data_df_to_persist = matched_data_df[matched_data_df["modified_flag"] == True]
        matched_data_df_to_persist["auto_matched"] = "N"
        raw_data_df_to_persist = raw_data_df_to_persist.drop(["modified_flag"], axis=1, )
        matched_data_df_to_persist = matched_data_df_to_persist.drop(["modified_flag"], axis=1)
        persist_df(raw_data_df_to_persist, "raw_data")
        persist_df(matched_data_df_to_persist, "matched_data")
    except Exception as e:
        traceback.print_exc(e)


def find_if_url_is_already_matched(matched_data_df, row):
    matched_data_df_filtered_on_token_count = matched_data_df[
        matched_data_df["token_count"] == row.token_count]
    match_found_flag = False
    matched_row_id = -1
    for index, filtered_row in enumerate(matched_data_df_filtered_on_token_count.itertuples(), 0):
        # We can just match on full URL's in matched_data_url and row
        if filtered_row.token_position == "":
            if filtered_row["potential_matched_url"] == row.potential_matched_url:
                match_found_flag = True
                matched_row_id = filtered_row.id
                break
            else:
                continue
        else:
            # Generate potential_row_url to match with filtered_row
            potential_row_url = get_potential_matched_url(row.tokens, filtered_row.token_position)
            if potential_row_url == filtered_row.potential_matched_url:
                # Since we already have a matched URL
                # Nothing else to do
                match_found_flag = True
                matched_row_id = filtered_row.id
                break
            else:
                continue
    return match_found_flag, matched_row_id


def handle_no_existing_matched_url_scenario(matched_data_df, raw_data_df, row):
    hamming_score, mismatch_token_index_str, key_id = get_best_hamming_score_for_df(raw_data_df, row.tokens,
                                                                                    row.token_count)
    if hamming_score == 0:
        # We found an exact match in raw_data_df
        # Update hit_count and mark modified_flag = True
        update_hit_count_value(raw_data_df, key_id)
    # If hamming Score = 1 then
    # We Found a potential match
    # Add row into matched_data_df
    # We update raw_data_df foreign key
    elif hamming_score == 1:
        matched_data_df = append_row_to_matched_df(hamming_score, matched_data_df, mismatch_token_index_str,
                                                   row, generate_csv_string_from_list(row.tokens))
    else:
        # If hamming_score > 1
        # Not handling such cases now
        # But we may want to support in future
        # So a TODO
        # For now in such case, it is just another raw_data with no match
        # so just insert in raw_data_df
        # When we are in the last iteration
        raw_data_df = append_row_to_raw_df(raw_data_df, row, generate_csv_string_from_list(row.tokens))
    return raw_data_df, matched_data_df


def generate_csv_string_from_list(list_to_convert):
    token_string = ','.join(list_to_convert)
    return token_string


def persist_df(dataframe, table_name):
    connection = cm.ManageConnection().get_connection()
    dataframe.to_sql(table_name, connection,if_exists="replace", index=False)


def initialize_data_frames():
    # Initialize data frames and add required columns
    raw_data_df = load_data_from_db("select * from raw_data")
    matched_data_df = load_data_from_db("select * from matched_data")
    raw_data_df[["matched_data_id"]] = raw_data_df[["matched_data_id"]].apply(pd.to_numeric)
    csv_log_file_df = pd.read_csv("../log_of_urls_invoked.csv")
    csv_log_file_df["tokens"] = csv_log_file_df["URL"].str.strip("'").str.strip('/').str.split('/')
    csv_log_file_df["token_count"] = csv_log_file_df["tokens"].str.len()
    raw_data_df["modified_flag"] = None
    matched_data_df["modified_flag"] = None
    return csv_log_file_df, matched_data_df, raw_data_df


def get_best_hamming_score_for_df(data_frame, token_list, token_count):
    # Filter raw_data_df using token_count as criteria
    # Should be a big performance boost in long run
    hamming_score = 0
    mismatch_token_index_str = ""
    key_id = -1
    df_filtered_on_token_count = data_frame[(data_frame.token_count == token_count)]
    # Get Hamming Score
    for index, raw_data_row in enumerate(df_filtered_on_token_count.itertuples(), 1):
        hamming_score, mismatch_token_index_str = get_hamming_score(raw_data_row.tokens.split(','),
                                                                    token_list)
        key_id = raw_data_row.id
        # If hamming Score = 0 then Current row matches exactly with a row in raw_data_df
        # In this case we just increase the hit count
        if hamming_score == 0 or hamming_score == 1:
            break
    return hamming_score, mismatch_token_index_str, key_id


def append_row_to_raw_df(raw_data_df, row, token_string):
    raw_data_dict = get_raw_data_dict(row, token_string)
    data_dict_df = pd.DataFrame(raw_data_dict, index=[id])
    raw_data_df = raw_data_df.append(data_dict_df, sort=True)
    return raw_data_df


def append_row_to_matched_df(hamming_score, matched_data_df, mismatch_token_index_str, row, token_string):
    potential_matched_url = get_potential_matched_url(row.tokens, mismatch_token_index_str)
    matched_data_dict = get_matched_data_dict(hamming_score, mismatch_token_index_str, potential_matched_url,
                                              row, token_string)
    matched_data_dict_to_df = pd.DataFrame(matched_data_dict, index=["id"])
    matched_data_df = matched_data_df.append(matched_data_dict_to_df, sort=True)
    return matched_data_df


def update_hit_count_value(data_frame, row_id):
    row_to_modify = data_frame.loc[data_frame["id"] == row_id]
    data_frame.loc[data_frame["id"] == row_id, ["hit_count", "modified_flag"]] = \
        row_to_modify['hit_count'].iloc[0] + 1, True
    return data_frame

url_matcher()
