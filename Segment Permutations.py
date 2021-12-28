##
#%%
import pandas as pd
import numpy as np
import seaborn as sns
import datetime
import random

# from datetime import timedelta -- commented out because of learning nesting
import regex as re

## to locate the csv we want
import os


#%%

###########################################
############# create time_key #############
###########################################

### NOTE: time_key contains all permutations of hh:mm to all other hh:mm

## Create all times between 00:00 to 23:59 -- 24 hours with 60 minutes easy per day is 1440 minutes
minute_start = np.arange(0, (24 * 60), 1)
minute_start = pd.DataFrame(minute_start)
minute_end = minute_start

## Create dataframe, & raw elapsed time
time_key = minute_start.merge(minute_end, how="cross")
time_key.rename(inplace=True, columns={"0_x": "start_time", "0_y": "end_time"})
time_key["active_time"] = time_key["end_time"] - time_key["start_time"]

# Create dates, correct for overnights & elapsed time
time_key["start_date"] = "2020-01-01"
time_key["start_date"] = pd.to_datetime(time_key["start_date"])
time_key["overnight"] = time_key["active_time"] < 0
time_key["end_date"] = np.where(
    time_key["overnight"],
    time_key["start_date"],
    time_key["start_date"] + datetime.timedelta(days=1),
)
time_key["active_time"] = np.where(
    time_key["overnight"], time_key["active_time"] + 1440, time_key["active_time"]
)


# reorder columns to be easier to read
time_key = time_key[
    ["start_date", "start_time", "end_date", "end_time", "active_time", "overnight"]
]

time_key["active_hours"] = round(time_key["active_time"] / 60, 4)

time_key.head(n=10)


###########################################
############ beautiful columns ############
###########################################

minutesInt_to_HHMM = lambda x: "{:02d}:{:02d}".format(*divmod(x, 60))

time_key["start_time"] = time_key["start_time"].apply(minutesInt_to_HHMM)
time_key["end_time"] = time_key["end_time"].apply(minutesInt_to_HHMM)

time_key["active_time"] = time_key["active_time"].apply(
    lambda x: datetime.timedelta(minutes=x)
)

time_key.sample(n=10)

# %%
###########################################
######### Load the template data ##########
###########################################

simData = pd.read_csv(
    "./Example Data/Simulated Timecard Data for 2 Employees.csv",
    dtype="str",
    parse_dates=["In.Actual.dt", "Out.Actual.dt"],
)

simData.rename(
    inplace=True,
    columns={
        "Person.ID": "shift_type",
        "In.Actual.dt": "clock_start",
        "Out.Actual.dt": "clock_end",
    },
)


simData["eeid"] = pd.to_numeric(
    simData["shift_type"].str.replace(r"\_", "", regex=True)
)
simData["shift_type"] = np.where(simData["shift_type"] == "_000001", "type1", "type2")

## Artificially segment each shift_type into the 1XXXX & the 2XXXX groups.
simData["eeid"] = np.where(
    simData["shift_type"] == "type1", simData["eeid"] + 10000, simData["eeid"] + 20000
)


# %%
###########################################
########### Create the 'class' ############
###########################################
class_size_input = 25  # NOTE: [final_class_size = (2 * class_size_input)]
class_size_multiplier = np.arange(0, class_size_input, 1)
simData_large = []

for i in class_size_multiplier:

    if i == 0:
        simData_large = simData.copy()
        simData_large["eeid"] = simData_large["eeid"] + i
    else:
        temp = simData.copy()
        temp["eeid"] = temp["eeid"] + i

        simData_large = simData_large.merge(temp, how="outer")

print("There are", simData_large["eeid"].nunique(), "eeids are in the data.")

# %%
###########################################
######### Make class more unique ##########
###########################################

## Let's add some randomness now that we have more unique eeids -- make it 'unique'
simData_large["clock_start"] = simData_large["clock_start"] + pd.to_timedelta(
    np.random.randint(0, 8, simData_large.shape[0]), unit="minute"
)

simData_large["clock_end"] = simData_large["clock_end"] + pd.to_timedelta(
    np.random.randint(9, 25, simData_large.shape[0]), unit="minute"
)

simData_large["date"] = simData_large["clock_start"].apply(lambda x: x.date())

## set these columns as index, sort by index, reset index to integer
### This sorts the entire dataframe & keeps our desired setup
simData_large = simData_large.set_index(["eeid", "clock_start"])
simData_large = simData_large.sort_index().reset_index()
simData_large = simData_large[
    ["shift_type", "eeid", "date", "clock_start", "clock_end"]
]

simData_large.head(n=15)

# %%
###########################################
######## Merge time_key into data #########
###########################################

## create the "HH:MM" merge key
simData_large["start_time"] = simData_large["clock_start"].dt.strftime("%H:%M")
simData_large["end_time"] = simData_large["clock_end"].dt.strftime("%H:%M")

df_analyze = simData_large.merge(
    time_key[["start_time", "end_time", "active_time", "active_hours"]],
    how="left",
    on=["start_time", "end_time"],
)

df_analyze.drop(columns="shift_type", inplace=True)
# df_analyze.head(n=15)


# %%
###########################################
###### eval time to next/last event #######
###########################################
df_analyze["clock_start_lead1"] = df_analyze.groupby("eeid")[["clock_start"]].apply(
    lambda x: x.shift(-1)  ## pull it back 1, so it is the row's last start
)
df_analyze["gap_next"] = df_analyze["clock_start_lead1"] - df_analyze["clock_end"]
df_analyze["gap_next"] = df_analyze["gap_next"].fillna(pd.Timedelta(hours=24 * 99))
df_analyze = df_analyze.drop(columns="clock_start_lead1")


df_analyze["clock_end_lag1"] = df_analyze.groupby("eeid")[["clock_end"]].apply(
    lambda x: x.shift(1)  ## pull it forward 1, so it is the row's next end
)
df_analyze["gap_last"] = df_analyze["clock_start"] - df_analyze["clock_end_lag1"]
df_analyze["gap_last"] = df_analyze["gap_last"].fillna(pd.Timedelta(hours=24 * 99))
df_analyze = df_analyze.drop(columns="clock_end_lag1")


#%%
###########################################
############ Cluster the events ###########
###########################################

## Row wise -- pull the start of any new shifts if gap_last > hours(4.5)
df_analyze["cluster_start"] = np.where(
    (df_analyze["gap_last"] > pd.Timedelta(hours=4.5)),
    df_analyze["clock_start"].astype(str),
    np.NAN,
)

## Populate these values downward
df_analyze["cluster_start"] = df_analyze.groupby("eeid")["cluster_start"].fillna(
    method="ffill"
)

## take the max of the clock_end value -- get the last clock out for each cluster
df_analyze["cluster_end"] = df_analyze.groupby(["eeid", "cluster_start"])[
    "clock_end"
].transform("max")

df_analyze.head(10)

# %%
###########################################
########## Examine Processed Data #########
###########################################

df_analyze.filter(
    regex=("eeid|^clock|^active_time|^gap_next|^cluster")
).drop_duplicates().head(n=10)

