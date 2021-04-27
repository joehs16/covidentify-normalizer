import pandas as pd
from pandas import json_normalize
import psycopg2
import psycopg2.extras
import json
import os
from azureml.core import Workspace, Dataset
from azureml.core.authentication import ServicePrincipalAuthentication
from AzureNormalizer import *
import configparser
import logging
import sys
import datetime as dt
import os

def main():
    #filepaths
    csv_filepath = 'input_csv/participants-covidAndFlu.csv'
    config_filepath = 'workspace_config/cfg.ini'
    
    #start timer
    tic_total = dt.datetime.now()

    # set up logger
    timestamp = dt.datetime.utcnow().strftime('%Y%m%d_%H-%M-%S')

    # establish log directory if doesn't exist
    os.makedirs("logs", exist_ok=True)
    filename=f'logs/run_{timestamp}.log'
    formatter = logging.Formatter('[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(filename=filename)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)

    # The handlers have to be at a root level since they are the final output
    logging.basicConfig(
        level=logging.INFO, 
        format='[{%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
        handlers=[
            file_handler,
            stream_handler
        ]
    )

    # define logger name
    logger = logging.getLogger('ETL Pipeline Logger')

    """
    Connect to PSQL
    """
    #read ini file
    config = configparser.ConfigParser()
    config.read(config_filepath)

    #set ini sections to variable for cleaner code
    svc_cfg = config['ServicePrincipalAuthentication']
    ws_cfg = config['Azure_Workspace']
    prompt_cfg = config['prompt_SQL_login']

    # initialize Authentication
    svc_pr = ServicePrincipalAuthentication(
        tenant_id = svc_cfg['tenant_id'],
        service_principal_id = svc_cfg['service_principal_id'],
        service_principal_password = svc_cfg['service_principal_password']
    )

    # initialize Azure workspace
    ws = Workspace(
        subscription_id = ws_cfg['subscription_id'],
        resource_group = ws_cfg['resource_group'],
        workspace_name = ws_cfg['workspace_name'],
        auth= svc_pr
    )

    # due to SQL query, must covert to tuple
    partipants_lst = tuple(
        pd.read_csv(csv_filepath,)
        .participant_id
        .values
    )

    #get pipeline prompt names from AzureNormalizer
    promptNames = whichPromptNames(
        device = any, 
        timePeriod = any,
        pipeline = True
    )
    
    #loop through each prompt
    for prompt in promptNames:
        tic = dt.datetime.now()

        logger.info(f"Starting query for '{prompt}'")

        try:
            connection = psycopg2.connect(
                host = "prompt-from-heroku.postgres.database.azure.com",
                dbname = "prompt",
                user = prompt_cfg['username'],
                password = prompt_cfg['password'],
                sslmode = "require",
                port = "5432",)
            cursor = connection.cursor('cursor_unique_name', cursor_factory=psycopg2.extras.DictCursor)
            
            """
            SQL QUERY
            """
            
            cursor.execute(f"""
                SELECT id, json_value, participant_id, prompt_name
                FROM covid_responses
                WHERE json_value IS NOT NULL
                AND participant_id IN {partipants_lst}
                AND prompt_name = '{prompt}'
                    """)  

            #empty dataframe
            output_df = pd.DataFrame()
            count = 0
            
            while True:
                count += 1
                rows = cursor.fetchmany(10)

                if rows:
                    print(count)
                    temp_df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description]) 
                    output_df = output_df.append(temp_df, ignore_index=True)

                else:
                    break

            logger.info(f'The shape of the output query is {output_df.shape}')

        except(Exception, psycopg2.Error) as error:
            logger.warning(f'Error connecting to PostgreSQL database {error}')
            connection = None

        # Close the database connection
        finally:
            if(connection != None):
                cursor.close()
                connection.close()
                logger.info(f'PostgreSQL connection is now closed')
        logger.info(f'finished querying!')
        
        """
        PERFORM NORMALIZATION PROCESS
        """
        logger.info(f'Starting json normalization for prompt = "{prompt}"')
        json_normalizer(
            device = "both",
            timePeriod = "both",
            promptName = prompt,
            data = output_df,
            aggSummary=False,
            ignoreInputs=True,
            prefix="pipeline",
            pandas=True,
            pipeline=True,
            workspace = ws
        )
        logger.info(f'Completed json normalization for prompt "{prompt}"')
        toc = dt.datetime.now()
        logger.info(f">>> Time elapsed for {prompt} was {toc-tic}")
        pass

    logger.info(">>> All uploads complete!")

    # end timer
    toc_total = dt.datetime.now()
    logger.info(f">>> Total time elapsed was {toc_total-tic_total}")


if __name__ == "__main__":
    main()