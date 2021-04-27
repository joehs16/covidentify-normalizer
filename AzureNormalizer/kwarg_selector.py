"""
kwarg_selector
This function defines the keyword arguments of each prompt name for ingestion\
    primarily by pd.json_normalize. This is needed because of the complicated\
    nested json structures.

INPUTS
device: either "garmin" or "fitbit"
timePeriod: Either "historical" or "current". When users enrolled into study,\
    their "historical" data was shared, "current" data is collected and\
    uploaded to the prompt database.
aggSummary (optional): for some garmin promptNames, it makes more sense to\
    display the data in the aggregated summaries form, which is one level\
    above the default summaries.

OUTPUTS
json_normalize_kwargs: Returns the associated keyword arguements.
"""
def kwarg_selector(device, timePeriod, aggSummary):
    if aggSummary is False:
        json_normalize_kwargs = {
            ### FITBIT HISTORICAL INPUTS ###
            "covid_fitbit_heart_rate_intraday_backfill": {
                "record_path": ["activities-heart", "value", "heartRateZones"],
                "meta": [
                    ["captured_at", "dateTime"],
                    ["activities-heart", "value", "restingHeartRate"],
                ],
                "record_prefix": "hRZ-",
                "errors": "ignore",
            },
            "covid_fitbit_steps_intraday_backfill": {
                "record_path": ["activities-steps"]
            },
            "covid_fitbit_floors_intraday_backfill": {
                "record_path": ["activities-floors"]
            },
            "covid_fitbit_elevation_intraday_backfill": {
                "record_path": ["activities-elevation"]
            },
            "covid_fitbit_distance_intraday_backfill": {
                "record_path": ["activities-distance"]
            },
            "covid_fitbit_calories_intraday_backfill": {
                "record_path": ["activities-calories"]
            },
            "covid_fitbit_sleep_intraday_backfill": {
                "record_path": ["sleep"]},
            ### FITBIT CURRENT INPUTS ###
            "heart_rate_intraday": {
                "record_path": ["dataset"],
                "meta": ["captured_at"]
            },
            "sleep": {},
            "calories_intraday": {
                "record_path": ["dataset"],
                "meta": ["captured_at"]
            },       
            "distance_intraday": {
                "record_path": ["dataset"],
                "meta": ["captured_at"]
            },
            "elevation_intraday": {
                "record_path": ["dataset"],
                "meta": ["captured_at"]
            },
            "floors_intraday": {
                "record_path": ["dataset"],
                "meta": ["captured_at"]
            },
            "steps_intraday": {
                "record_path": ["dataset"],
                "meta": ["captured_at"]
            },
            "r15_daily_sleep": {
                "record_path": ["sleep"],
                "meta": ["captured_at"]
            },
            "weekly_r15_sleep_api": {
                "record_path": ["sleep"],
                "meta": ["captured_at"]
            },
            "weekly_r15_water_api": {
                "record_path": ["water"],
                "meta": ["captured_at"]
            },
            "weekly_r15_food_api": {
                "record_path": ["foods"],
                "meta": ["captured_at"]
            },
            ### GARMIN INPUTS ###
            "covid_garmin_activity": {
                "record_path": "activities",
                "meta": "captured_at",
            },
            "covid_garmin_daily": {
                "record_path": "dailies", 
                "meta": "captured_at"},
            "covid_garmin_epoch": {
                "record_path": "epoches", 
                "meta": "captured_at"},
            "covid_garmin_sleep": {
                "record_path": "sleep", "meta": "captured_at"},
        }
        pass
    else: #aggSummary is True
        json_normalize_kwargs = {
            ### FITBIT CURRENT INPUTS ###
            "r15_daily_sleep": {},
            "weekly_r15_sleep_api": {},
            "weekly_r15_water_api": {},
            "weekly_r15_food_api": {},
            ### GARMIN INPUTS ###
            "covid_garmin_activity": {},
            "covid_garmin_daily": {},
            "covid_garmin_epoch": {},
            "covid_garmin_sleep": {},
        }
        pass
    return json_normalize_kwargs

### DEPRECATED BUT KEEPING JUST IN CASE NEED TO SWITCH BACK###
# """
# kwarg_selector
# This function defines the keyword arguments of each prompt name for ingestion\
#     primarily by pd.json_normalize. This is needed because of the complicated\
#     nested json structures.

# INPUTS
# device: either "garmin" or "fitbit"
# timePeriod: Either "historical" or "current". When users enrolled into study,\
#     their "historical" data was shared, "current" data is collected and\
#     uploaded to the prompt database.
# aggSummary (optional): for some garmin promptNames, it makes more sense to\
#     display the data in the aggregated summaries form, which is one level\
#     above the default summaries.

# OUTPUTS
# json_normalize_kwargs: Returns the associated keyword arguements.
# """
# def kwarg_selector(device, timePeriod, aggSummary):
#     if (device == "fitbit") and (timePeriod == "historical"):
#         json_normalize_kwargs = {
#             "covid_fitbit_heart_rate_intraday_backfill": {
#                 "record_path": ["activities-heart", "value", "heartRateZones"],
#                 "meta": [
#                     ["captured_at", "dateTime"],
#                     ["activities-heart", "value", "restingHeartRate"],
#                 ],
#                 "record_prefix": "hRZ-",
#                 "errors": "ignore",
#             },
#             "covid_fitbit_steps_intraday_backfill": {
#                 "record_path": ["activities-steps"]
#             },
#             "covid_fitbit_floors_intraday_backfill": {
#                 "record_path": ["activities-floors"]
#             },
#             "covid_fitbit_elevation_intraday_backfill": {
#                 "record_path": ["activities-elevation"]
#             },
#             "covid_fitbit_distance_intraday_backfill": {
#                 "record_path": ["activities-distance"]
#             },
#             "covid_fitbit_calories_intraday_backfill": {
#                 "record_path": ["activities-calories"]
#             },
#             "covid_fitbit_sleep_intraday_backfill": {"record_path": ["sleep"]},
#         }
#     elif (device == "fitbit") and (timePeriod == "current"):
#         if aggSummary is False:
#             json_normalize_kwargs = {
#                 "heart_rate_intraday": {
#                     "record_path": ["dataset"],
#                     "meta": ["captured_at"]
#                 },
#                 "sleep": {},
#                 "calories_intraday": {
#                     "record_path": ["dataset"],
#                     "meta": ["captured_at"]
#                 },       
#                 "distance_intraday": {
#                     "record_path": ["dataset"],
#                     "meta": ["captured_at"]
#                 },
#                 "elevation_intraday": {
#                     "record_path": ["dataset"],
#                     "meta": ["captured_at"]
#                 },
#                 "floors_intraday": {
#                     "record_path": ["dataset"],
#                     "meta": ["captured_at"]
#                 },
#                 "steps_intraday": {
#                     "record_path": ["dataset"],
#                     "meta": ["captured_at"]
#                 },
#                 "r15_daily_sleep": {
#                     "record_path": ["sleep"],
#                     "meta": ["captured_at"]
#                 },
#                 "weekly_r15_sleep_api": {
#                     "record_path": ["sleep"],
#                     "meta": ["captured_at"]
#                 },
#                 "weekly_r15_water_api": {
#                     "record_path": ["water"],
#                     "meta": ["captured_at"]
#                 },
#                 "weekly_r15_food_api": {
#                     "record_path": ["foods"],
#                     "meta": ["captured_at"]
#                 },
#             }
#         else: #aggSummary is True
#             json_normalize_kwargs = {
#                 "r15_daily_sleep": {},
#                 "weekly_r15_sleep_api": {},
#                 "weekly_r15_water_api": {},
#                 "weekly_r15_food_api": {},
#             }
#     else:  # garmin
#         if (aggSummary is False) and (timePeriod == "current"):
#             json_normalize_kwargs = {
#                 "covid_garmin_activity": {
#                     "record_path": "activities",
#                     "meta": "captured_at",
#                 },
#                 "covid_garmin_daily": {"record_path": "dailies", "meta": "captured_at"},
#                 "covid_garmin_epoch": {"record_path": "epoches", "meta": "captured_at"},
#                 "covid_garmin_sleep": {"record_path": "sleep", "meta": "captured_at"},
#             }
#         elif (aggSummary is True) and (timePeriod == "current"):
#             json_normalize_kwargs = {
#                 "covid_garmin_activity": {},
#                 "covid_garmin_daily": {},
#                 "covid_garmin_epoch": {},
#                 "covid_garmin_sleep": {},
#             }

#         else:
#             pass
#     return json_normalize_kwargs

