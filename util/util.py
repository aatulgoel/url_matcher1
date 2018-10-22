import traceback

import pandas as pd

from connection_manager import oracle_connection_manager as cm


def get_primary_key():
    connection = cm.ManageConnection().get_connection()
    raw_data_id_result = connection.execute("select raw_data_seq.nextval as seqval from dual")
    raw_data_id = None
    for seq_result in raw_data_id_result:
        raw_data_id = seq_result.seqval
    return raw_data_id


def load_data_from_db(sql_stmt):
    try:
        connection = cm.ManageConnection().get_connection()
        df = pd.read_sql(sql_stmt, con=connection)
        return df
    except Exception as e:
        traceback.print_exc(e)


def get_hamming_score(list1, list2):
    hamming_score = 0
    mismatch_position = ""
    for index in range(len(list1)):
        if list1[index] != list2[index]:
            hamming_score += 1
            mismatch_position = str(index) + ","
    mismatch_position = mismatch_position.strip(',')
    return hamming_score, mismatch_position


def get_raw_data_dict(row):
    raw_data_dict = {"id": get_primary_key(),
                     "raw_url": row.URL,
                     "hit_count": 1,
                     "service_providing_system": row.sourceIP,
                     "service_using_system": row.appName,
                     "token_count": row.token_count,
                     "tokens": generate_csv_string_from_list(row.tokens),
                     "modified_flag": True}
    return raw_data_dict


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
                         "modified_flag": True,
                         "hit_count": 1}
    return matched_data_dict


def get_potential_matched_url(tokens, mismatch_position):
    mismatch_position_list = mismatch_position.split(',')
    mismatch_position_list = [int(x) for x in mismatch_position_list]
    tokens = [x if index not in mismatch_position_list else '{place holder}' for index, x in enumerate(tokens, 0)]
    potential_matched_url = "/".join(tokens)
    potential_matched_url = "/" + potential_matched_url
    return potential_matched_url


def generate_csv_string_from_list(list_to_convert):
    token_string = ','.join(list_to_convert)
    return token_string