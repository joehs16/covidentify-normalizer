import pandas as pd
import json
from pandas import json_normalize
from .azureWriter import azureWriter
from .kwarg_selector import kwarg_selector

"""
json_normalizer

This function will extract an Azure Dataset from a Azure Dataset Consume\
transform the nested json into a relational table and load the transformed\
table directly into the Azure dataSets Assets list.

INPUTS
device: either "garmin" or "fitbit"
timePeriod: Either "historical" or "current". When users enrolled into study,\
    their "historical" data was shared, "current" data is collected and\
    uploaded to the prompt database.
promptName: name of the type of data collected.
data: dataset from the targeted Azure DataSet Consume.
aggSummary (optional): for some garmin promptNames, it makes more sense to\
    display the data in the aggregated summaries form, which is one level\
    above the default summaries.
ignoreInputs (optional): if turned on, skips the default user inputs.
prefix (optional): used in conjunction when 'ignoreInputs' is turned on to\
    set the uploaded filename's prefix.
pandas (optional): Allows using a pandas dataFrame instead of using a Azure dataset.\
    There is a performance boost when converting to a pandas dataFrame outside\
    of the function.
pipeline (optional): designates if the 

OUTPUTS
Automatically uploads the transformed data into the Azure dataSets Assets list.
"""

def json_normalizer(
    device,
    timePeriod,
    promptName,
    data,
    workspace,
    aggSummary=False,
    ignoreInputs=False,
    prefix=None,
    pandas=False,
    pipeline=False,
):
    if pipeline == False:
        if pandas == False:
            # converts Azure dataset into pandas dataframe
            data = data.to_pandas_dataframe()
        else:
            pass

        # selects corresponding kwargs based on inputs
        json_normalize_kwargs = kwarg_selector(device, timePeriod, aggSummary)

        # filter for prompt_name
        subsetForTransformation = data[(data.prompt_name == promptName)]

        # create an empty dataframe to store data
        output_df = pd.DataFrame()

        # numbers of rows of data
        rowsOfData = subsetForTransformation.shape[0]

        # list to capture rows without data
        noData = []

        for row in range(rowsOfData):
            try:
                # saves the current_id and participant_id from the parent dataframe
                current_id, current_participant_id = (
                    subsetForTransformation.id.iloc[row],
                    subsetForTransformation.participant_id.iloc[row],
                )

                # select correct data transformation based on promptName
                if (device == "fitbit") and (timePeriod == "historical"):
                    _data = json.loads(subsetForTransformation["json_value"].iloc[row])

                # garmin data has characters that need to be removed before transformation
                else:
                    chars = '"'
                    _data = json.loads(
                        subsetForTransformation["json_value"]
                        .iloc[row]
                        .replace("\\", "")
                        .strip(chars)
                    )

                # converts the json_normalize output into a temp pandas dataframe
                pd_current_json_value = pd.DataFrame(
                    json_normalize(data=_data, **json_normalize_kwargs[promptName])
                )

                # loop to drop extra columns based on prompt name
                if (aggSummary is False) and (device == "garmin"):
                    if promptName == "covid_garmin_sleep":
                        pd_current_json_value = pd_current_json_value.loc[
                            :,
                            ~pd_current_json_value.columns.str.startswith(
                                "timeOffsetSleep"
                            ),
                        ]
                    elif promptName == "covid_garmin_daily":
                        pd_current_json_value = pd_current_json_value.loc[
                            :,
                            ~pd_current_json_value.columns.str.startswith(
                                "timeOffsetHeartRateSamples"
                            ),
                        ]
                    else:  # placeholder for future prompt names
                        pass
                else:
                    pass

                # added the current id and current participant_id to the current temp dataframe
                pd_current_json_value = pd_current_json_value.assign(
                    uniqueRowID=current_id, participant_id=current_participant_id
                )

                # appending temp dataframe to total dataframe
                output_df = output_df.append(pd_current_json_value)

            # if there is a row without data, will throw an error
            except:
                noData.append(row)
                continue

        # reset index
        output_df = output_df.reset_index(drop=True)

        # dealing with no data in output
        if output_df.shape == (0,0):
            raise ValueError("No data in output dataframe. Check inputs.")
        else:
            print(">>> The shape of the output dataframe is: ", output_df.shape)
            print(">>> The number of rows that did not have data are:", len(noData))
            if ignoreInputs == False:
                while input("Do you want see a sample? [y/n]") == "y":
                    print(output_df.sample(n=len(output_df) * 10, replace=True))
                    break
            else:
                pass

        # Azure Write Pipeline
        if ignoreInputs == False:
            while input("Do you want to upload? [y/n]") == "y":
                prefix = input("What do you want the prefix for the file to be?")
                azureWriter(device, timePeriod, promptName, aggSummary, output_df, prefix, workspace, pipeline)
                break
        else:
            azureWriter(device, timePeriod, promptName, aggSummary, output_df, prefix, workspace, pipeline)
            pass
    else: ### pipeline code ###
        if pandas == False:
            # converts Azure dataset into pandas dataframe
            data = data.to_pandas_dataframe()
        else:
            pass

#         # selects corresponding kwargs based on inputs
#         json_normalize_kwargs = kwarg_selector(device, timePeriod, aggSummary)

        # filter for prompt_name
        subsetForTransformation = data[(data.prompt_name == promptName)]

        # create an empty dataframe to store data
        output_df = pd.DataFrame()

        # numbers of rows of data
        rowsOfData = subsetForTransformation.shape[0]

        # list to capture rows without data
        noData = []
        
        if promptName == "covid_fitbit_heart_rate_intraday_backfill":
            for row in range(rowsOfData):
                try:
                    # saves the current_id and participant_id from the parent dataframe
                    current_participant_id = subsetForTransformation.participant_id.iloc[row]
                    singleRow = subsetForTransformation.json_value.iloc[row]

                    postSingleRow = json_normalize(
                                data=
                                    singleRow,
                                    record_path = "activities-heart")

                    pd_current_json_value = pd.DataFrame(postSingleRow)
                    pd_current_json_value = pd_current_json_value.loc[
                                                :,[
                                                    'dateTime',
                                                    'value.restingHeartRate',
                                                ]
                                            ]
                    pd_current_json_value = pd_current_json_value.assign(
                        participant_id=current_participant_id
                    )
                    # appending temp dataframe to total dataframe
                    output_df = output_df.append(pd_current_json_value)
                except:
                            noData.append(row)
                            continue
                # reset index
                output_df = output_df.reset_index(drop=True)
                
        elif promptName == "covid_fitbit_steps_intraday_backfill":
            for row in range(rowsOfData):
                try:
                    # saves the current_id and participant_id from the parent dataframe
                    current_participant_id = subsetForTransformation.participant_id.iloc[row]
                    singleRow = subsetForTransformation.json_value.iloc[row]

                    postSingleRow = json_normalize(
                                data=
                                    singleRow,
                                    record_path = "activities-steps")

                    pd_current_json_value = pd.DataFrame(postSingleRow)

                    pd_current_json_value = pd_current_json_value.assign(
                        participant_id=current_participant_id
                    )
                    # appending temp dataframe to total dataframe
                    output_df = output_df.append(pd_current_json_value)
                except:
                            noData.append(row)
                            continue

                # reset index
                output_df = output_df.reset_index(drop=True)
                
        elif promptName == "covid_fitbit_sleep_intraday_backfill":
            for row in range(rowsOfData):
                try:
                    # saves the current_id and participant_id from the parent dataframe
                    current_participant_id = subsetForTransformation.participant_id.iloc[row]
                    singleRow = subsetForTransformation.json_value.iloc[row]

                    postSingleRow = json_normalize(
                                data=
                                    singleRow,
                                    record_path = "sleep")

                    pd_current_json_value = pd.DataFrame(postSingleRow)
                    pd_current_json_value = pd_current_json_value.loc[
                                                :,[
                                                    'dateOfSleep',
                                                    'endTime',
                                                    'duration',
                                                    'startTime',
                                                    'minutesAwake',
                                                    'minutesToFallAsleep',
                                                ]
                                            ]
                    pd_current_json_value = pd_current_json_value.assign(
                        participant_id=current_participant_id
                    )
                    # appending temp dataframe to total dataframe
                    output_df = output_df.append(pd_current_json_value)
                except:
                            noData.append(row)
                            continue

                # reset index
                output_df = output_df.reset_index(drop=True)

        elif promptName == "covid_garmin_daily":
            for row in range(rowsOfData):
                try:
                    # saves the current_id and participant_id from the parent dataframe
                    current_participant_id = subsetForTransformation.participant_id.iloc[row]
                    singleRow = subsetForTransformation.json_value.iloc[row]
                    singleRow = json.loads(singleRow)
                    postSingleRow = json_normalize(
                                data=
                                    singleRow,
                                    record_path = "dailies")

                    pd_current_json_value = pd.DataFrame(postSingleRow)
                    pd_current_json_value = pd_current_json_value.loc[
                                                :,[
                                            "calendarDate",
                                            "activityType",
                                            "restingHeartRateInBeatsPerMinute",
                                            "steps",
                                            ]
                                        ]
                    pd_current_json_value = pd_current_json_value.assign(
                        participant_id=current_participant_id
                    )
                    # appending temp dataframe to total dataframe
                    output_df = output_df.append(pd_current_json_value)
                except:
                            noData.append(row)
                            continue

                # reset index
                output_df = output_df.reset_index(drop=True)
                
        elif promptName == "covid_garmin_sleep":
            for row in range(rowsOfData):
                try:
                    # saves the current_id and participant_id from the parent dataframe
                    current_participant_id = subsetForTransformation.participant_id.iloc[row]
                    singleRow = subsetForTransformation.json_value.iloc[row]
                    singleRow = json.loads(singleRow)
                    postSingleRow = json_normalize(
                                data=
                                    singleRow,
                                    record_path = "sleep")

                    pd_current_json_value = pd.DataFrame(postSingleRow)
                    pd_current_json_value = pd_current_json_value.loc[
                                                :,[
                                                    'calendarDate',
                                                    'validation',
                                                    'durationInSeconds',
                                                    'remSleepInSeconds',
                                                    'startTimeInSeconds',
                                                    'awakeDurationInSeconds',
                                                    'deepSleepDurationInSeconds',
                                                    'unmeasurableSleepInSeconds',
                                                    'lightSleepDurationInSeconds',
                                                ]
                    ]
                    pd_current_json_value = pd_current_json_value.assign(
                        participant_id=current_participant_id
                    )
                    # appending temp dataframe to total dataframe
                    output_df = output_df.append(pd_current_json_value)
                except:
                            noData.append(row)
                            continue

                # reset index
                output_df = output_df.reset_index(drop=True)

        else:
            pass    
    
        # dealing with no data in output
        if output_df.shape == (0,0):
            raise ValueError("No data in output dataframe. Check inputs.")
#         else:
#             print(">>> The shape of the output dataframe is: ", output_df.shape)
#             print(">>> The number of rows that did not have data are:", len(noData))
#             if ignoreInputs == False:
#                 while input("Do you want see a sample? [y/n]") == "y":
#                     print(output_df.sample(n=len(output_df) * 10, replace=True))
#                     break
#             else:
#                 pass

#         # Azure Write Pipeline
#         if ignoreInputs == False:
#             while input("Do you want to upload? [y/n]") == "y":
#                 prefix = input("What do you want the prefix for the file to be?")
#                 azureWriter(device, timePeriod, promptName, aggSummary, output_df, prefix, pipeline)
#                 break
        else:
            azureWriter(device, timePeriod, promptName, aggSummary, output_df, prefix, workspace = workspace, pipeline=True,)
            pass

"""
whichPromptNames

This function is to help users identify which prompt names are available in\
json_normalizer function. This is manually inputted and if new prompt names are\
added, this needs to be updated.

INPUTS
device: either "garmin" or "fitbit".
timePeriod: Either "historical" or "current". When users enrolled into study,\
    their "historical" data was shared, "current" data is collected and\
    uploaded to the prompt database.

OUTPUTS
Outputs a list of prompt names.
"""

def whichPromptNames(device,timePeriod, pipeline = False):
    if pipeline == True:
        lst = ["covid_fitbit_sleep_intraday_backfill",
               "covid_fitbit_heart_rate_intraday_backfill",
               "covid_fitbit_steps_intraday_backfill",
               "covid_garmin_daily",
               "covid_garmin_sleep",
              ]
        return lst
    else:
        if (device == "fitbit") and (timePeriod == "historical"):
            lst = ["covid_fitbit_heart_rate_intraday_backfill",
                 "covid_fitbit_steps_intraday_backfill",
                 "covid_fitbit_floors_intraday_backfill",
                 "covid_fitbit_floors_intraday_backfill",
                 "covid_fitbit_elevation_intraday_backfill",
                 "covid_fitbit_distance_intraday_backfill",
                 "covid_fitbit_calories_intraday_backfill",
                 "covid_fitbit_sleep_intraday_backfill"]
            return lst
        elif (device == "fitbit") and (timePeriod == "current"):
            lst = ["heart_rate_intraday",
                   "sleep",
                   "calories_intraday",
                   "distance_intraday",
                   "elevation_intraday",
                   "floors_intraday",
                   "steps_intraday",
                   "r15_daily_sleep",
                   "weekly_r15_sleep_api",
                   "weekly_r15_water_api",
                   "weekly_r15_food_api"]
            return lst
        else:
            lst = ["covid_garmin_activity", 
             "covid_garmin_daily", 
             "covid_garmin_epoch",
             "covid_garmin_sleep"]
            return lst