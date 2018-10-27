import pandas as pd

from connection import oracle as cm
from constants import MATCHED_DATA_UPDATE_STRING, RAW_DATA_UPDATE_STRING
from main import sqlcol
from utils import get_potential_matched_url, get_hamming_score, get_raw_data_dict, get_matched_data_dict


def find_if_url_is_already_matched(matched_data_df, row):
    """

    :param matched_data_df:
    :param row:
    :return:
    """
    matched_data_df_filtered_on_token_count = matched_data_df[
        matched_data_df["token_count"] == row.token_count]
    match_found_flag = False
    matched_row_id = -1
    for index, filtered_row in enumerate(matched_data_df_filtered_on_token_count.itertuples(), 0):
        # We can just match on full URL's in matched_data_url and row
        if set(filtered_row.tokens.split(',')) & set(row.tokens) != set([]):
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


def persist_df(data_frame, table_name):
    data_frame = data_frame
    df_insert = data_frame[data_frame["already_exists_in_db"] != True]
    df_insert.drop(["already_exists_in_db"], inplace=True, axis=1)
    df_update = data_frame[data_frame["already_exists_in_db"] == True]
    output_dict = sqlcol(data_frame)
    connection = cm.ManageConnection().get_connection()
    df_insert.to_sql(table_name, connection, if_exists="append", index=False, dtype=output_dict)

    if table_name == "raw_data":
        update_statement = get_update_stmt(table_name)
        for row_to_update in df_update.itertuples():
            connection.execute(update_statement, (row_to_update.hit_count,
                                                  row_to_update.matched_data_id,
                                                  row_to_update.raw_url,
                                                  row_to_update.service_providing_system,
                                                  row_to_update.service_using_system,
                                                  row_to_update.token_count,
                                                  row_to_update.tokens,
                                                  row_to_update.id))
    elif table_name == "matched_data":
        update_statement = get_update_stmt(table_name)
        for row_to_update in df_update.itertuples():
            connection.execute(update_statement, (row_to_update.potential_matched_url,
                                                  row_to_update.hamming_score,
                                                  row_to_update.tokens,
                                                  row_to_update.hit_count,
                                                  row_to_update.token_position,
                                                  row_to_update.token_count,
                                                  row_to_update.service_providing_system,
                                                  row_to_update.final_matched_url,
                                                  row_to_update.auto_matched,
                                                  row_to_update.auto_matched_verified,
                                                  row_to_update.false_positive,
                                                  row_to_update.housekeep_raw_data,
                                                  row_to_update.id))


def get_update_stmt(table_name):
    raw_data_update_str = RAW_DATA_UPDATE_STRING

    matched_data_update_str = MATCHED_DATA_UPDATE_STRING

    if table_name == "raw_data":
        return raw_data_update_str
    elif table_name == "matched_data":
        return matched_data_update_str
    else:
        print("Error")


def get_best_hamming_score_for_df(raw_data_df, row):
    # Filter raw_data_df using token_count as criteria
    # Should be a big performance boost in long run
    hamming_score = -1
    mismatch_token_index_str = ""
    key_id = -1
    df_filtered_on_token_count = raw_data_df[
        (raw_data_df["token_count"] == row.token_count) & (raw_data_df["matched_data_id"].isnull())]
    # Get Hamming Score
    for index, raw_data_row in enumerate(df_filtered_on_token_count.itertuples(), 1):
        hamming_score, mismatch_token_index_str = get_hamming_score(raw_data_row.tokens.split(','),
                                                                    row.tokens)
        key_id = raw_data_row.id
        # If hamming Score = 0 then Current row matches exactly with a row in raw_data_df
        # In this case we just increase the hit count
        if hamming_score == 0 or hamming_score == 1:
            break
    return hamming_score, mismatch_token_index_str, key_id


def append_row_to_raw_df(raw_data_df, row):
    check_if_row_exists = (
            (raw_data_df["raw_url"] == row.URL) & (raw_data_df["service_providing_system"] == row.sourceIP) & (
            raw_data_df["service_using_system"] == row.appName)).any()
    if not check_if_row_exists:
        raw_data_dict = get_raw_data_dict(row)
        data_dict_df = pd.DataFrame(raw_data_dict, index=[id])
        raw_data_df = raw_data_df.append(data_dict_df, sort=True)
    return raw_data_df


def append_row_to_matched_df(hamming_score, matched_data_df, mismatch_token_index_str, row, token_string):
    potential_matched_url = get_potential_matched_url(row.tokens, mismatch_token_index_str)
    matched_data_dict = get_matched_data_dict(hamming_score, mismatch_token_index_str, potential_matched_url,
                                              row, token_string)
    matched_data_dict_to_df = pd.DataFrame(matched_data_dict, index=["id"])
    matched_data_df = matched_data_df.append(matched_data_dict_to_df, sort=True)
    matched_data_id = matched_data_dict.get("id")
    return matched_data_df, matched_data_id


def update_hit_count_value(data_frame, row_id):
    row_to_modify = data_frame.loc[data_frame["id"] == row_id]
    data_frame.loc[data_frame["id"] == row_id, ["hit_count", "modified_flag"]] = \
        row_to_modify['hit_count'].iloc[0] + 1, True
    return data_frame


def create_matched_raw_data_link(raw_data_df, raw_data_id, matched_data_id):
    raw_data_df.loc[raw_data_df["id"] == raw_data_id, ["matched_data_id"]] = matched_data_id
    return raw_data_df