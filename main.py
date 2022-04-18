# %%
import logging
import pandas as pd
import sqlalchemy

from yaml import load, Loader


############# Kolkata Connection and Operations Methods ###############

def conn_kolkata():
    '''
    Connect to kolkata server
    :return obj connection:
    '''
    logger.info('connecting to database')
    # Try to connect to server
    try:
        cnxn = sqlalchemy.engine.url.URL.create(
            'mssql+pyodbc',
            username=config['credentials']['username'],
            password=config['credentials']['password'],
            host=config['credentials']['host'],
            port=config['credentials']['port'],
            database=config['credentials']['database'],
            query=dict(driver='SQL Server Native Client 11.0'))

        engine = sqlalchemy.create_engine(cnxn)
        connection = engine.connect()
        logger.info('connection successful')

    # Log errors
    except sqlalchemy.exc.InterfaceError as e:
        logger.error(f'{e} Failed to connect: Bad connection parameters')
    except ConnectionError as e:
        logger.error(f'{e} Failed to connect to server')
    return connection

def query_kolkata(cnxn, query):
    '''
    Query kolkata server
    :param Connection cnxn:
    :param str query:
    :return dataframe df:
    '''
    logger.info('Querying database')
    # Try querying database
    try:
        df = pd.read_sql_query(
            query, cnxn)
        logger.info('Query successful')
        df.sort_values(by=['ID'], inplace=True)
    except Exception as e:
        logger.error(f'{e}')
    cnxn.close()
    return df


def insert_kolkata(df, table):
    '''
    Insert dataframe of pipeline status into database, appending new records
    :param Dataframe df:
    :param String table:
    '''
    cnxn = conn_kolkata()
    try:
        df.to_sql(table, con=cnxn, if_exists='append', index=False)
    except Exception as e:
        logger.error(e)


def update_kolkata(df, table):
    '''
    Update database with status of pipeline
    :param Dataframe df:
    :param String table:
    '''
    cnxn = conn_kolkata()
    # Get dataframe column names in a list
    cols = df.columns.values.tolist()
    # Create string of column names and values to be updated
    placeholder = ', '.join([f"{c} = '{df.loc[0, c]}'" for c in cols])
    # Build sql update statement string formatted with table name, placeholder, and ref_id
    sql = f"UPDATE {table} SET {placeholder} WHERE ref_id = '{df.loc[0, 'ref_id']}';"
    try:
        cnxn.execute(sql)
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':

    # Open Config File
    with open('config.yaml', 'r') as config_file:
        config = load(config_file, Loader=Loader)

    # setup logging
    logging.basicConfig(filename='log/corporate_wellness_solutions.log', filemode='a',
                        format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('corporate_wellness_solutions.log')
    if config['mode'] != 'production':
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARN)
    logger.info('setting up config')
    logger.info('running in {0} mode'.format(config['mode']))

    # Create a Query and Connection to kolkata for all Workday employee records
    wkdy_idm_emp_query = '''select * 
from CACHED_IDM_STUDENTS WHERE CACHED_IDM_STUDENTS.CLASSIFICATION <> 'Alumni' AND ID NOT IN ( SELECT ID FROM CACHED_IDM_EMPLOYEES WHERE CLASSIFICATION NOT LIKE 'Retired%')
AND ID <> '1692683' -- exclude Paul Steber, per Robert Sawyer'''
    cnxn = conn_kolkata()

# %%

    # Execute query and get a pandas dataframe of the data
    wkdy_idm_emp_df = query_kolkata(cnxn, wkdy_idm_emp_query)
    
    wkdy_idm_emp_df
# %%
