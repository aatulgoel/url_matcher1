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

                raw_data_id_result = connection.execute("select raw_data_seq.nextval as seqval from dual")

                for seq_result in raw_data_id_result:
                    raw_data_id = seq_result.seqval

                token_string = ', '.join(row.tokens)

                data_dict = {"id": raw_data_id, "raw_url": row.URL, "hit_count": 1, \
                              "service_providing_system":row.sourceIP, "service_using_system": row.appName,\
                             "token_count":row.token_count, "tokens":token_string}
                data_dict_df = pd.DataFrame(data_dict, index=[id])

                raw_data_df.append(data_dict_df, sort=True)

                data_dict_df.to_sql('RAW_DATA', connection, if_exists='append', index=False)

                print(data_dict_df.head())
    except Exception as e:
        traceback.print_exc(e)


url_matcher()