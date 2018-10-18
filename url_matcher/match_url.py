import connection_manager.oracle_connection_manager as cm
import traceback
import pandas as pd


def load_data_from_db(sql_stmt):
    try:
        connection = cm.ManageConnection().get_connection()
        df = pd.read_sql(sql_stmt, con=connection)
        return df
    except Exception as e:
        traceback.print_exc(e)


def get_primary_key():
    connection = cm.ManageConnection().get_connection()
    raw_data_id_result = connection.execute("select raw_data_seq.nextval as seqval from dual")
    raw_data_id = None
    for seq_result in raw_data_id_result:
        raw_data_id = seq_result.seqval
    return raw_data_id


def get_hamming_score(list1, list2):
    hamming_score = 0
    mismatch_position = ""
    for index in range(len(list1)):
        if list1[index] != list2[index]:
            hamming_score += 1
            mismatch_position = str(index) + ","
    mismatch_position = mismatch_position.strip(',')
    return hamming_score, mismatch_position


def get_potential_matched_url(tokens, mismatch_postion):
    mismatch_postion_list = mismatch_postion.split(',')
    mismatch_postion_list = [int(x) for x in mismatch_postion_list]
    tokens = ['{place holder}' if index not in mismatch_postion_list else x for index, x in enumerate(tokens, 0)]
    potential_matched_url = "/".join(tokens)
    potential_matched_url = "/" + potential_matched_url
    return potential_matched_url


def url_matcher():
    try:
        # Initialize dataframes and add required columns
        raw_data_df = load_data_from_db("select * from raw_data")
        matched_data_df = load_data_from_db("select * from matched_data")
        csv_log_file_df = pd.read_csv("../log_of_urls_invoked.csv")
        csv_log_file_df["tokens"] = csv_log_file_df["URL"].str.strip("'").str.strip('/').str.split('/')
        csv_log_file_df["token_count"] = csv_log_file_df["tokens"].str.len()
        raw_data_df["modified_flag"] = None
        matched_data_df["modified_flag"] = None

        # Iterate over csv file to find matches
        for row in csv_log_file_df.itertuples():
            # If both matched_data_df and raw_data_df are blank means the System is new
            # So we just insert the first fow of csv_log_file_df into raw_data_df
            if len(matched_data_df) == 0 and len(raw_data_df) == 0:
                # Create a dictionary
                # Convert dictionary to a dataframe
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
                pass
    except Exception as e:
        traceback.print_exc(e)


def check_raw_data_match(matched_data_df, raw_data_df, row):
    # Filter raw_data_df using token_count as criteria
    # Should be a big performance boost in long run
    df_filtered_on_token_count = raw_data_df[(raw_data_df.token_count == row.token_count)]
    # Loop through df_filtered_on_token_count to find a potential match
    # Get Hamming Score
    df_filtered_on_token_count_size = df_filtered_on_token_count.shape[0]
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
            if df_filtered_on_token_count_size == index:
                raw_data_dict = get_raw_data_dict(row, token_string)
                data_dict_df = pd.DataFrame(raw_data_dict, index=[id])
                raw_data_df = raw_data_df.append(data_dict_df, sort=True)
    return matched_data_df, raw_data_df


def get_matched_data_dict(hamming_score, mismatch_token_index_str, potential_matched_url, row, token_string):
    matched_data_dict = {"id": get_primary_key(),
                         "potential_matched_url": potential_matched_url,
                         "hamming_score": hamming_score,
                         "tokens": token_string,
                         "token_position": mismatch_token_index_str,
                         "token_count": row.token_count,
                         "service_providing_system": row.sourceIP,
                         "final_matched_url": None,
                         "auto_matched": None,
                         "auto_matched_verified": None,
                         "false_positive": None,
                         "housekeep_raw_data": None,
                         "modified_flag": True}
    return matched_data_dict


def get_raw_data_dict(row, token_string):
    raw_data_dict = {"id": get_primary_key(),
                     "raw_url": row.URL,
                     "hit_count": 1,
                     "service_providing_system": row.sourceIP,
                     "service_using_system": row.appName,
                     "token_count": row.token_count,
                     "tokens": token_string,
                     "modified_flag": True}
    return raw_data_dict


url_matcher()
