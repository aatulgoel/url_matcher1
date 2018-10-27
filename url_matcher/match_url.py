import traceback

import pandas as pd
import sqlalchemy

from connection_manager import oracle_connection_manager as cm
from util.util import load_data_from_db, get_hamming_score, get_raw_data_dict, get_matched_data_dict, \
    get_potential_matched_url, generate_csv_string_from_list


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

        raw_data_df_to_persist = raw_data_df[raw_data_df["modified_flag"] == True]
        matched_data_df_to_persist = matched_data_df[matched_data_df["modified_flag"] == True]
        matched_data_df_to_persist["auto_matched"] = "N"
        raw_data_df_to_persist = raw_data_df_to_persist.drop(["modified_flag"], axis=1, )
        matched_data_df_to_persist = matched_data_df_to_persist.drop(["modified_flag"], axis=1)

        persist_df(matched_data_df_to_persist, "matched_data")
        persist_df(raw_data_df_to_persist, "raw_data")
    except Exception as e:
        traceback.print_exc(e)


def find_if_url_is_already_matched(matched_data_df, row):
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


def handle_no_existing_matched_url_scenario(matched_data_df, raw_data_df, row):
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


def persist_df(data_frame, table_name):
    data_frame = data_frame
    df_insert = data_frame[data_frame["already_exists_in_db"] != True]
    df_insert.drop(["already_exists_in_db"], inplace=True, axis=1)
    df_update = data_frame[data_frame["already_exists_in_db"] == True]
    print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

    print("df_update number of rows = ", df_update.shape[0])
    print(data_frame.dtypes)
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
    raw_data_update_str = "update raw_data set  \
                                hit_count = :hit_count,\
                                matched_data_id =:matched_data_id,\
                                raw_url =:raw_url,\
                                service_providing_system =:service_providing_system,\
                                service_using_system =:service_using_system,\
                                token_count =:token_count,\
                                tokens =:tokens \
                                where id = :id"

    matched_data_update_str = "update matched_data set  \
                            potential_matched_url = :potential_matched_url,\
                            hamming_score =:hamming_score,\
                            tokens =:tokens,\
                            hit_count =:hit_count,\
                            token_position =:token_position,\
                            token_count =:token_count,\
                            service_providing_system =:service_providing_system, \
                            final_matched_url =:final_matched_url, \
                            auto_matched =:auto_matched, \
                            auto_matched_verified =:auto_matched_verified, \
                            false_positive =:false_positive, \
                            housekeep_raw_data =:housekeep_raw_data \
                            where id = :id"
    if table_name == "raw_data":
        return raw_data_update_str
    elif table_name == "matched_data":
        return matched_data_update_str
    else:
        print("Error")


def sqlcol(data_frame_params):
    data_type_dict = {}
    for i, j in zip(data_frame_params.columns, data_frame_params.dtypes):
        if "object" in str(j):
            data_type_dict.update({i: sqlalchemy.types.VARCHAR(length=1000)})
        if "datetime" in str(j):
            data_type_dict.update({i: sqlalchemy.types.DateTime()})
        if "float" in str(j):
            data_type_dict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
        if "int" in str(j):
            data_type_dict.update({i: sqlalchemy.types.INT()})

    return data_type_dict


def initialize_data_frames():
    # Initialize data frames and add required columns
    raw_data_df = load_data_from_db("select * from raw_data")
    raw_data_df = raw_data_df.fillna({"matched_data_id": ''})
    raw_data_df["already_exists_in_db"] = True
    matched_data_df = load_data_from_db("select * from matched_data")
    matched_data_df["already_exists_in_db"] = True

    # raw_data_df[["id", "token_count", "hit_count", "matched_data_id"]] = raw_data_df[
    #     ["id", "token_count", "hit_count", "matched_data_id"]].astype(np.int64, errors='ignore')
    # matched_data_df[["id", "token_count", "hit_count", "hamming_score"]] = matched_data_df[
    #     ["id", "token_count", "hit_count", "hamming_score"]].astype(np.int64, errors='ignore')

    csv_log_file_df = pd.read_csv("../log_of_urls_invoked.csv")
    csv_log_file_df["tokens"] = csv_log_file_df["URL"].str.strip("'").str.strip('/').str.split('/')
    csv_log_file_df["token_count"] = csv_log_file_df["tokens"].str.len()
    raw_data_df["modified_flag"] = None
    matched_data_df["modified_flag"] = None
    return csv_log_file_df, matched_data_df, raw_data_df


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


url_matcher()
