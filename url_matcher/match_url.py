import connection_manager.oracle_connection_manager as cm
import traceback
import pandas as pd


def load_data_from_db(sql_stmt):
    try:
        conn_handler = cm.ManageConnection()
        conn = conn_handler.create_connection()

        df = pd.read_sql(sql_stmt, con=conn)
        return df

    except Exception as e:
        traceback.print_exc(e)

    finally:
        conn_handler.distroy_connection()

def url_matcher():
    conn_handler = cm.ManageConnection()
    conn = conn_handler.create_connection()
    try:
        raw_data_df = load_data_from_db("select * from raw_data")
        matched_data_df = load_data_from_db("select * from matched_data")

        data_to_match_df = pd.read_csv("../log_of_urls_invoked.csv")

        print(data_to_match_df.head(5))

        for row in data_to_match_df.itertuples():
            if len(matched_data_df) == 0 and len(raw_data_df) == 0:
                data_dict = {"id": 1, "raw_url": row.URL, "hit_count": "1", \
                              "service_providing_system":row.sourceIP, "service_using_system": row.appName,"token_count":"1",\
                              "matched_data_id":"1"}
                print(data_dict)
                # data_dict_df = pd.DataFrame.from_dict(data_dict, orient='index')
                data_dict_df = pd.DataFrame(data_dict, index=[0])

                raw_data_df.append(data_dict_df, ignore_index=False)

                raw_data_df.to_sql('RAW_DATA', conn, if_exists='replace')

                print(data_dict_df.head())


    except Exception as e:
        traceback.print_exc(e)


url_matcher()