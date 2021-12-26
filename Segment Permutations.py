#%%
import pandas as pd
import numpy as np
import seaborn as sns
import datetime

# from datetime import timedelta -- commented out because of learning nesting
import regex as re

## to locate the csv we want
import os


#%%

## Create all times between 00:00 to 23:59 -- 24 hours with 60 minutes easy per day is 1440 minutes
minute_start = np.arange(0, (24 * 60), 1)
minute_start = pd.DataFrame(minute_start)
minute_end = minute_start

## Create dataframe, & raw elapsed time
minutes = minute_start.merge(minute_end, how="cross")
minutes.rename(inplace=True, columns={"0_x": "start_time", "0_y": "end_time"})
minutes["time_elapsed"] = minutes["end_time"] - minutes["start_time"]

# Create dates, correct for overnights & elapsed time
minutes["start_date"] = "2020-01-01"
minutes["start_date"] = pd.to_datetime(minutes["start_date"])
minutes["overnight"] = minutes["time_elapsed"] < 0
minutes["end_date"] = np.where(
    minutes["overnight"],
    minutes["start_date"],
    minutes["start_date"] + datetime.timedelta(days=1),
)
minutes["time_elapsed"] = np.where(
    minutes["overnight"], minutes["time_elapsed"] + 1440, minutes["time_elapsed"]
)


# reorder columns to be easier to read
minutes = minutes[
    ["start_date", "start_time", "end_date", "end_time", "time_elapsed", "overnight"]
]

minutes.sample(n=10)

# %%

## create a more civil view of the minutes
##TODO: write a function to replace these repetitive lambda statements
### NOTE: This converts the minute integer to a 5 digit string HH:MM

### NOTE: How do I convert the start_date + minutes(start_time) into a vector?
### Reason here is to convert yyyy-mm-dd hh:mm into a HH:MM string is easy.

minutes["start_time"] = minutes["start_time"].apply(
    lambda x: "{:02d}:{:02d}".format(*divmod(x, 60))
)
minutes["end_time"] = minutes["end_time"].apply(
    lambda x: "{:02d}:{:02d}".format(*divmod(x, 60))
)
minutes["time_elapsed"] = minutes["time_elapsed"].apply(
    lambda x: "{:02d}:{:02d}".format(*divmod(x, 60))
)


minutes.sample(n=10)
# %%

simData = pd.read_csv(
    "./Example Data/Simulated Timecard Data for 2 Employees.csv",
    dtype="str",
    parse_dates=["In.Actual.dt", "Out.Actual.dt"],
)

simData.rename(
    inplace=True,
    columns={
        "Person.ID": "eeid",
        "In.Actual.dt": "clock_start",
        "Out.Actual.dt": "clock_end",
    },
)


simData["start_time"] = simData["clock_start"].dt.strftime("%H:%M")
simData["end_time"] = simData["clock_end"].dt.strftime("%H:%M")

simData.info()


# %%

simData.head()

# %%

simData.sample(n=10)

# %%

simData2 = simData.merge(
    minutes[["start_time", "end_time", "time_elapsed", "overnight"]],
    how="left",
    on=["start_time", "end_time"],
)

simData2.sample(n=15)


# %%

# %%
