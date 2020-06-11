import datetime
import time
import xml.etree.ElementTree as ET
import pandas as pd
import requests
import sqlalchemy as sql
import re
import numpy as np
import pg8000
import psycopg2
import logging

### e_apx_gcp_daily

#pd.set_option('display.max_columns', None)

def initial_set_up():

    todays_date = datetime.date.today()
    number_of_historical_date_to_check = 5
    hist_start_date = todays_date - datetime.timedelta(days=number_of_historical_date_to_check)
    hist_end_date = todays_date

    date_index = [hist_end_date - datetime.timedelta(n) for n in range(number_of_historical_date_to_check)]

    return_dict = {}
    return_dict['date_index'] = date_index
    return_dict['hist_start_date'] = hist_start_date
    return_dict['hist_end_date'] = hist_end_date

    #print(return_dict)

    return return_dict


def rpd_data_backfill(start, end, submit_to_sql_toggle = 1, auto_one_desktop_zero = 1):

    ### Backfill historically manually using this script.
    ### PULL UTC TIME CONVERSION
    if auto_one_desktop_zero:
        engine = sql.create_engine('postgres+pg8000://pgsqlstageernest:6r33nfr0G@/gcp_staging?unix_sock=/cloudsql/viridis-stage-1:europe-west2:postgre-sql-staging/.s.PGSQL.5432')
    else:
        engine = sql.create_engine('postgres+psycopg2://pgsqlstageernest:6r33nfr0G@35.197.199.134:5432/gcp_staging')

    connection = engine.connect()

    utc = pd.read_sql_query('SELECT * FROM time_conversion.time_conversion_tbl;', engine)[['utc_datetime', 'uk_datetime', 'uk_hh', 'uk_extra_hour', 'uk_date', 'utc_date']]
    utc['uk_date'] = pd.to_datetime(utc['uk_date'])
    utc['utc_date'] = pd.to_datetime(utc['utc_date'])


    #utc.info()
    #print(utc)

    connection.close()
    engine.dispose()

    enappsys_username = 'GFPT_quant'
    enappsys_password = '6r33nfr0G'

    start = start.strftime('%Y%m%d')
    end = end.strftime('%Y%m%d')

    ## 4 services from enappsys
    ## GB_APX_RPD_PRICE, GB_APX_RPD_VOLUME, GB_APX_HH_RPD_PRICE, GB_APX_HH_RPD_VOLUME
    rpd_price_url = f'https://www.netareports.com/dataService?rt=aws&username={enappsys_username}&password={enappsys_password}&datetimetype=HH&timezone=UTC&utcstartdatetime={start}000000&utcenddatetime={end}000000&inclusive=true&datatype=GB_APX_RPD_PRICE'
    rpd_volume_url = f'https://www.netareports.com/dataService?rt=aws&username={enappsys_username}&password={enappsys_password}&datetimetype=HH&timezone=UTC&utcstartdatetime={start}000000&utcenddatetime={end}000000&inclusive=true&datatype=GB_APX_RPD_VOLUME'
    rpd_hh_price_url = f'https://www.netareports.com/dataService?rt=aws&username={enappsys_username}&password={enappsys_password}&datetimetype=HH&timezone=UTC&utcstartdatetime={start}000000&utcenddatetime={end}000000&inclusive=true&datatype=GB_APX_HH_RPD_PRICE'
    rpd_hh_volume_url = f'https://www.netareports.com/dataService?rt=aws&username={enappsys_username}&password={enappsys_password}&datetimetype=HH&timezone=UTC&utcstartdatetime={start}000000&utcenddatetime={end}000000&inclusive=true&datatype=GB_APX_HH_RPD_VOLUME'

    rpd_price = requests.post((rpd_price_url))
    rpd_volume = requests.post((rpd_volume_url))
    rpd_hh_price = requests.post((rpd_hh_price_url))
    rpd_hh_volume = requests.post((rpd_hh_volume_url))


    ## rpd_price ##
    e_rpd_price = ET.fromstring(rpd_price.text)
    bb = []
    for i in e_rpd_price:
        bb.append({**i.attrib})

    rpd_price_df = pd.DataFrame(bb)
    rpd_price_df = rpd_price_df.rename(columns={'value': 'rpd_price'})

    ## rpd_volume ##
    e_rpd_volume = ET.fromstring(rpd_volume.text)
    bb = []
    for i in e_rpd_volume:
        bb.append({**i.attrib})

    rpd_volume_df = pd.DataFrame(bb)
    rpd_volume_df = rpd_volume_df.rename(columns={'value': 'rpd_volume'})

    ## rpd_hh_price ##
    e_rpd_hh_price = ET.fromstring(rpd_hh_price.text)
    bb = []
    for i in e_rpd_hh_price:
        bb.append({**i.attrib})

    rpd_hh_price_df = pd.DataFrame(bb)
    rpd_hh_price_df = rpd_hh_price_df.rename(columns={'value': 'rpd_hh_price'})

    ## rpd_hh_volume ##
    e_rpd_hh_volume = ET.fromstring(rpd_hh_volume.text)
    bb = []
    for i in e_rpd_hh_volume:
        bb.append({**i.attrib})

    rpd_hh_volume_df = pd.DataFrame(bb)
    rpd_hh_volume_df = rpd_hh_volume_df.rename(columns={'value': 'rpd_hh_volume'})

    #print(rpd_price_df)

    combi_df = pd.merge(rpd_price_df, rpd_volume_df, on=['utcstartdatetime'], left_index=True, suffixes=('_x', '_y'))
    combi_df_2 = pd.merge(combi_df, rpd_hh_price_df, on=['utcstartdatetime'], left_index=True, suffixes=('_y', '_y'))
    combi_df_3 = pd.merge(combi_df_2, rpd_hh_volume_df, on=['utcstartdatetime'], left_index=True, suffixes=('_y', '_z'))

    combi_df_3 = combi_df_3[['utcstartdatetime', 'utcenddatetime_x', 'updatedbyimporter_x', 'rpd_price', 'rpd_volume', 'rpd_hh_price', 'rpd_hh_volume']]
    #print(combi_df_3)
    combi_df_3['utcstartdatetime'] = pd.to_datetime(combi_df_3['utcstartdatetime'])

    #
    rpd_df = utc.merge(combi_df_3, left_on=['utc_datetime'], right_on=['utcstartdatetime'], how='right')

    rpd_df['status_id'] = 1
    rpd_df['override_rpd_price'] = np.nan
    rpd_df['override_rpd_volume'] = np.nan
    rpd_df['override_rpd_hh_price'] = np.nan
    rpd_df['override_rpd_hh_volume'] = np.nan
    rpd_df['utc_insert_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())

    rpd_df = rpd_df.rename(columns={'utcenddatetime_x': 'utcenddatetime', 'updatedbyimporter_x':'updatedbyimporter'})

    print(rpd_df)

    if submit_to_sql_toggle:

        if auto_one_desktop_zero:
            #engine = sql.create_engine('postgres+pg8000://pgsqlstageernest:6r33nfr0G@/gcp_staging?unix_sock=/cloudsql/viridis-stage-1:europe-west2:postgre-sql-staging/.s.PGSQL.5432')
            engine = sql.create_engine('postgres+pg8000://pgadmin1:zerotablesnofood@/thalya_testing_enapsys?unix_sock=/cloudsql/viridis-testing:europe-west2-a:postgre-sql-1/.s.PGSQL.5432')
        else:
            engine = sql.create_engine('postgres+psycopg2://pgsqlstageernest:6r33nfr0G@35.197.199.134:5432/gcp_staging')

        connection = engine.connect()

        try:
            rpd_df.to_sql(name='rpd_history', con=connection, if_exists='append', index=False, schema='enappsys_intraday', chunksize=10000)
        except:
            logging.error(f'e_Failed to add APX data to database of date start {start} to {end}')

        connection.close()
        engine.dispose()

    return rpd_df


def auto_check_submit(self, auto_one_desktop_zero = 1):

    initial_setup = initial_set_up()
    check_start = initial_setup['hist_start_date']
    check_end = initial_setup['hist_end_date']

    if auto_one_desktop_zero:
        #engine = sql.create_engine('postgres+pg8000://pgsqlstageernest:6r33nfr0G@/gcp_staging?unix_sock=/cloudsql/viridis-stage-1:europe-west2:postgre-sql-staging/.s.PGSQL.5432')
        engine = sql.create_engine('postgres+pg8000://pgadmin1:zerotablesnofood@/thalya_testing_enapsys?unix_sock=/cloudsql/viridis-testing:europe-west2-a:postgre-sql-1/.s.PGSQL.5432')
    else:
        engine = sql.create_engine('postgres+psycopg2://pgsqlstageernest:6r33nfr0G@35.197.199.134:5432/gcp_staging')

    connection = engine.connect()
    rpd_data_date = pd.read_sql_query(f"SELECT DISTINCT(utc_date) FROM enappsys_intraday.rpd_history WHERE utc_date BETWEEN '{check_start}' AND '{check_end}' ORDER BY utc_date;", engine)

    connection.close()
    engine.dispose()

    print(rpd_data_date)

    for date_to_loop in initial_setup['date_index']:
        print(f'date_to_loop:{date_to_loop}')
        print(f'set:{set(pd.to_datetime(rpd_data_date["utc_date"]))}')
        if pd.to_datetime(date_to_loop) in set(pd.to_datetime(rpd_data_date['utc_date'])):
            pass
        else:
            start = date_to_loop
            end = date_to_loop + datetime.timedelta(days=1)
            try:
                if auto_one_desktop_zero:
                    rpd_df = rpd_data_backfill(start, end, 1,1)
                else:
                    rpd_df = rpd_data_backfill(start, end, 1, 0)
                logging.info(f'e_Successful: RPD data to database of date start {start} to {end}')
            except:
                logging.error(f'e_Failed to add RPD data to database of date start {start} to {end}')

    logging.info("e_enappsys_apx finished")
    #return 1

# if __name__ == "__main__":
#     rpd_data_backfill(datetime.date(2020, 1,1), datetime.date(2020, 6,6),1,0)
#     #auto_check_submit(request=0, auto_one_desktop_zero = 0)
#     #check_date_and_submit_apx_data()