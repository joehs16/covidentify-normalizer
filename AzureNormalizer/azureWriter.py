import os
from azureml.core import Workspace, Dataset

"""
azureWriter
This function automatically uploads the transformed data into the Azure\
    dataSets Assets list. Used in conjunction with json_normalizer.py

INPUTS
device: either "garmin" or "fitbit"
timePeriod: Either "historical" or "current". When users enrolled into study,\
    their "historical" data was shared, "current" data is collected and\
    uploaded to the prompt database.
promptName: name of the type of data collected.
aggSummary (optional): for some garmin promptNames, it makes more sense to\
    display the data in the aggregated summaries form, which is one level\
    above the default summaries.
output_df: this is the transformed dataframe that is ready to be uploaded.
prefix: used in to set the uploaded filename's prefix.

OUTPUTS
Automatically uploads the transformed data into the Azure dataSets Assets list.
"""
def azureWriter(device, timePeriod, promptName, aggSummary, output_df, prefix, workspace, pipeline=False):
    #print(device, timePeriod, promptName, "aggSummary =", aggSummary)
    print(">>> Starting upload...")
    if pipeline == False:
        
#         # removing 'covid_' from the string
#         promptName = promptName.split("_", 1)[1]

        # create directory if directory doesn't exist
        os.makedirs("output_csv", exist_ok=True)
        #save CSV locally
        output_df.to_csv(f"output_csv/{prefix}-{promptName}.csv", index=False)
        print(f"./output_csv/{prefix}-{promptName}.csv")

        # connect to datastore
        datastore = workspace.get_default_datastore()

        # upload
        datastore.upload_files(
            files=[f"./output_csv/{prefix}-{promptName}.csv"],
            target_path="output_csv/",
            overwrite=True,
            show_progress=True,
        )
        ds = Dataset.Tabular.from_delimited_files(
            datastore.path(f"output_csv/{prefix}-{promptName}.csv")
        )
        ds.register(
            workspace=workspace,
            name=f"{prefix}-{promptName}",
            description=f"This data is from the 'Covid Positive Participant' dataset.\n\
        ---Details---\n\
        >>> The device: {device}\n\
        >>> The prompt name is: {promptName}\n\
        >>> The time period is: {timePeriod}\n\
        >>> Is aggregated summary?: {aggSummary}",
            create_new_version=True,
        )
        print(">>> Upload Successful!")
    else:
        # create directory if directory doesn't exist
        os.makedirs("data_covidentify_analysis_db", exist_ok=True)
        
        #save CSV locally
        output_df.to_csv(f"data_covidentify_analysis_db/{prefix}-{promptName}.csv", index=False)
        print(f"./data_covidentify_analysis_db/{prefix}-{promptName}.csv")    

        # connect to datastore
        datastore = workspace.get_default_datastore()

        # upload
        datastore.upload_files(
            files=[f"./data_covidentify_analysis_db/{prefix}-{promptName}.csv"],
            target_path="data_covidentify_analysis_db/",
            overwrite=True,
            show_progress=True,
        )

        print(f">>> Upload of {promptName} Successful!")