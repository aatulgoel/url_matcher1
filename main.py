import traceback

import pandas as pd

from helpers import find_if_url_is_already_matched, persist_df, get_best_hamming_score_for_df, append_row_to_raw_df, \
    append_row_to_matched_df, update_hit_count_value, create_matched_raw_data_link
from utils import load_data_from_db, generate_csv_string_from_list


def persist_data_frames(matched_data_df, raw_data_df):
    """
    Persists matched data frame and raw data frame to database
    :param matched_data_df:
    :param raw_data_df:
    :return:
    """
    raw_data_df_to_persist = raw_data_df[raw_data_df["modified_flag"] == True]
    matched_data_df_to_persist = matched_data_df[matched_data_df["modified_flag"] == True]
    matched_data_df_to_persist["auto_matched"] = "N"
    raw_data_df_to_persist = raw_data_df_to_persist.drop(["modified_flag"], axis=1, )
    matched_data_df_to_persist = matched_data_df_to_persist.drop(["modified_flag"], axis=1)
    persist_df(matched_data_df_to_persist, "matched_data")
    persist_df(raw_data_df_to_persist, "raw_data")


def handle_no_existing_matched_url_scenario(matched_data_df, raw_data_df, row):
    """
    When no match is found in matched data frame, this function tries to find a match in raw_data_frame
    If raw_data_frame match is found then a row is added in matched_data
    and raw_data is updated with foreign key on matched data.
    If no match is found in raw data then a row is added in raw data
    :param matched_data_df:
    :param raw_data_df:
    :param row:
    :return:
    """
    hamming_score, mismatch_token_index_str, raw_data_id = get_best_hamming_score_for_df(raw_data_df, row)
    if hamming_score == 0:
        # We found an exact match in raw_data_df
        # Update hit_count and mark modified_flag = True
        update_hit_count_value(raw_data_df, raw_data_id)
    # If hamming Score = 1 then
    # We Found a potential match
    # Add row into matched_data_df
    # We update raw_data_df foreign key
    elif hamming_score == 1:
        matched_data_df, matched_data_id = append_row_to_matched_df(hamming_score, matched_data_df,
                                                                    mismatch_token_index_str,
                                                                    row, generate_csv_string_from_list(row.tokens))
        raw_data_df = create_matched_raw_data_link(raw_data_df, raw_data_id, matched_data_id)
    else:
        # If hamming_score > 1
        # Not handling such cases now
        # But we may want to support in future
        # So a TODO
        # For now in such case, it is just another raw_data with no match
        # so just insert in raw_data_df
        # When we are in the last iteration
        raw_data_df = append_row_to_raw_df(raw_data_df, row)
    return raw_data_df, matched_data_df


def initialize_data_frames():
    """

    :return:
    """
    # Initialize data frames and add required columns
    raw_data_df = load_data_from_db("select * from raw_data")
    raw_data_df = raw_data_df.fillna({"matched_data_id": ''})
    raw_data_df["already_exists_in_db"] = True
    matched_data_df = load_data_from_db("select * from matched_data")
    matched_data_df["already_exists_in_db"] = True
    csv_log_file_df = pd.read_csv("./data/log_of_urls_invoked.csv")
    csv_log_file_df["tokens"] = csv_log_file_df["URL"].str.strip("'").str.strip('/').str.split('/')
    csv_log_file_df["token_count"] = csv_log_file_df["tokens"].str.len()
    raw_data_df["modified_flag"] = None
    matched_data_df["modified_flag"] = None
    return csv_log_file_df, matched_data_df, raw_data_df


def url_matcher():
    try:
        # Initialize data frames
        csv_log_file_df, matched_data_df, raw_data_df = initialize_data_frames()

        for row in csv_log_file_df.itertuples():
            # If both matched_data_df and raw_data_df are blank means the system is new
            # So we just insert the first fow of csv_log_file_df into raw_data_df
            if len(matched_data_df.index) == 0 and len(raw_data_df.index) == 0:
                raw_data_df = append_row_to_raw_df(raw_data_df, row)

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

        persist_data_frames(matched_data_df, raw_data_df)
    except Exception as e:
        traceback.print_exc(e)


if __name__ == "__main__":
    url_matcher()
