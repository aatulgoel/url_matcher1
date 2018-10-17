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

def get_primary_key_id():
    connection = cm.ManageConnection().get_connection()
    raw_data_id_result = connection.execute("select raw_data_seq.nextval as seqval from dual")

    for seq_result in raw_data_id_result:
        raw_data_id = seq_result.seqval

    return raw_data_id

def get_hamming_score(list1,list2):
    hamming_score = 0
    for index in range(len(list1)):
        if list1[index] != list2[index]:
            hamming_score += 1
    return hamming_score


def url_matcher():
    connection = cm.ManageConnection().get_connection()
    try:
        raw_data_df = load_data_from_db("select * from raw_data")
        matched_data_df = load_data_from_db("select * from matched_data")
        data_to_match_df = pd.read_csv("../log_of_urls_invoked.csv")
        data_to_match_df["tokens"] = data_to_match_df["URL"].str.strip("'").str.strip('/').str.split('/')
        data_to_match_df["token_count"] = data_to_match_df["tokens"].str.len()

        for row in data_to_match_df.itertuples():
            if len(matched_data_df) == 0 and len(raw_data_df) == 0:
                token_string = ','.join(row.tokens)
                type(token_string)

                data_dict = {"id": get_primary_key_id(), "raw_url": row.URL, "hit_count": 1, \
                              "service_providing_system":row.sourceIP, "service_using_system": row.appName,\
                             "token_count":row.token_count, "tokens":token_string}
                data_dict_df = pd.DataFrame(data_dict, index=[id])

                data_dict_df.to_sql('RAW_DATA', connection, if_exists='append', index=False)

                raw_data_df = raw_data_df.append(data_dict_df, sort=True)

            elif len(matched_data_df) == 0:
                same_token_count_df = raw_data_df[(raw_data_df.token_count == row.token_count)]
                #print(row.tokens , "row of tokens")
                #print(raw_data_df.head())
                #print(raw_data_df["tokens"].str.split() , "token string")
                
                for same_token_count_df_row in same_token_count_df.itertuples():
                    hamming_score = get_hamming_score(same_token_count_df_row.tokens.split(','), row.tokens)

                print(hamming_score)





    except Exception as e:
        traceback.print_exc(e)


url_matcher()