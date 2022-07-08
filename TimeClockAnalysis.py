#%%
import datetime
from dataclasses import dataclass

import numpy as np
import pandas as pd
import regex as re
from dateutil.parser import parse

#%%

# ! TODO: Should I do this; @dataclass
class TimeClockAnalysis:
    def __init__(
        self,
        inputDataRowCount=int,
        inputDataSplitRowCount=int,
        ResetTimer=int,
        removeOverlaps=bool,
        dataKey=pd.Series,
        tsIn=datetime.datetime,
        tsOut=datetime.datetime,
        dfTime=pd.DataFrame(None),
        dfTC=pd.DataFrame(None),
        dfSH=pd.DataFrame(None),
    ):
        self.removeOverlaps = removeOverlaps

    #####
    ##### Set all variables used for later &
    def setVars(
        self,
        inputData: pd.DataFrame,
        IDKey: str,
        TimeStart: str,
        TimeEnd: str,
        ResetTimerMinutes: int,
        # TODO: add weekly/biweekly/bimonthly pay periods
        # TODO: start date for PPs - Maybe down to "yyyy-mm-dd hh:mm"
    ):

        ## check data type & set internally
        if not isinstance(inputData, pd.DataFrame):
            raise ValueError(
                "pd.DataFrame type expected but not found for 'inputData'"
            )
        self.inputDataRowCount = len(inputData.index)
        self.dfTime = inputData

        # check variable names exist within input data
        checkVars = [IDKey, TimeStart, TimeEnd]
        if not all(isinstance(val, str) for val in checkVars):
            raise ValueError(
                "String type expected but not found. Please check inputs for: IDKey, dateTimeStart, dateTimeEnd"
            )

        if not set(checkVars).issubset(set(inputData.columns)):
            raise ValueError(
                "Inputs are expected to match columns found in column names of data input."
            )
        ## set these as 'self.vars' for later
        self.dataKey = IDKey
        self.tsIn = TimeStart
        self.tsOut = TimeEnd

        if not isinstance(ResetTimerMinutes, int):
            raise ValueError(
                "int type expected but not found for 'ResetTimerMinutes'"
            )
        self.ResetTimerMinutes = ResetTimerMinutes

        return print("Ready for Analysis")

    def sortData(self):
        self.dfTime.sort_values(
            inplace=True,
            by=[self.dataKey, self.tsIn, self.tsOut],
            ascending=(True, True, False),
            # TODO: option to enable the third boolean to be True -- this would prioritize shorter segments if there are overlaps.
        )
        self.dfTime.reset_index(inplace=True, drop=True)

    def splitAtMidnight(self):

        self.sortData()

        ## split the data into two areas:
        ### (1) Data that DOES NOT cross over midnight
        dfTime_good_data = self.dfTime[
            self.dfTime[self.tsIn].dt.date
            == self.dfTime[self.tsOut].dt.date
        ]

        ### (2) Data that DOES cross over midnight
        dfTime_to_split = self.dfTime[
            self.dfTime[self.tsIn].dt.date
            != self.dfTime[self.tsOut].dt.date
        ]

        #### Create a pre-midnight segment via floor rounding
        dfTime_premidnight = dfTime_to_split.copy()
        dfTime_premidnight.reset_index(inplace=True, drop=True)
        dfTime_premidnight[self.tsOut] = dfTime_premidnight[
            self.tsOut
        ].dt.floor("D")
        ### TODO: Is it helpful to have this end on the final sec?- pd.Timedelta("1second")

        #### Create a post-midnight segment via ceil rounding
        dfTime_postmidnight = dfTime_to_split.copy()
        dfTime_postmidnight.reset_index(inplace=True, drop=True)
        dfTime_postmidnight[self.tsIn] = dfTime_postmidnight[
            self.tsIn
        ].dt.ceil("D")

        ### Combine pre & post midnight segments
        dfTime_split = dfTime_premidnight.merge(
            dfTime_postmidnight, how="outer"
        )

        ### Combine 'good' data & the split segments
        self.dfTime = dfTime_good_data.merge(dfTime_split, how="outer")
        self.inputDataSplitRowCount = len(self.dfTime.index)

        self.sortData()

        ## Now we drop all interim objects
        del (
            dfTime_good_data,
            dfTime_to_split,
            dfTime_premidnight,
            dfTime_postmidnight,
        )

    def createDateVar(self, usingTimeStart=True):
        if usingTimeStart:
            self.dfTime["date"] = self.dfTime[self.tsIn].apply(
                datetime.datetime.date
            )
        else:
            print("Date was created using the end timestamp")
            self.dfTime["Date"] = self.dfTime[self.tsOut].apply(
                datetime.datetime.date
            )

    #####
    ##### Export whatever data is availible -- intended for dev use only
    def exportDataInProgress(self):
        return self.dfTime.copy()

    #####
    ##### Export fully analyzed
    def exportTimecardData(self):
        if self.dfTC is None:
            raise ValueError("Timecard analysis is not fully completed.")
        return self.dfTC.copy()

    def exportShiftData(self):
        if self.dfSH is None:
            raise ValueError("Timecard analysis is not fully completed.")
        return self.dfSH.copy()

    #####
    ##### GapNext/GapLast
    def calcGapLastGapNext(self):
        ###
        ### This functions creates two variables gapLast & gapNext
        ### gapLast - Time from tsIn to the prior rows's tsOut
        ### gapNext - Time from tsOut to the next row's tsOut
        ###
        fillna_with_this = pd.Timedelta(hours=24 * 99)

        #### The next block is used to calculate gapLast

        self.dfTime["tsOut_lag1"] = self.dfTime.groupby(self.dataKey)[
            [self.tsOut]
        ].apply(
            lambda x: x.shift(1)
            ## move the row 'forward' 1 to get the prior value
        )
        self.dfTime["gapLast"] = (
            self.dfTime[self.tsIn] - self.dfTime["tsOut_lag1"]
        )
        self.dfTime["gapLast"].fillna(fillna_with_this, inplace=True)

        ####
        #### The next block is used to calculate gapNext
        self.dfTime["tsIn_lead1"] = self.dfTime.groupby(self.dataKey)[
            [self.tsIn]
        ].apply(
            lambda x: x.shift(
                -1
            )  ## move the rows 'back' 1 to get the leading value
        )

        self.dfTime["gapNext"] = (
            self.dfTime["tsIn_lead1"] - self.dfTime[self.tsOut]
        )

        self.dfTime["gapNext"].fillna(fillna_with_this, inplace=True)

        self.dfTime.drop(
            columns=["tsOut_lag1", "tsIn_lead1"], inplace=True
        )
        del fillna_with_this

    #####
    ##### Identify, quantiy & then remove non-linear time segments
    def validateGapTiming(self):

        zero_gap_time = pd.Timedelta(minutes=0)
        ## TODO: min_gap_time could be set by user as variable phantomgap
        min_gap_time = pd.Timedelta(minutes=10)

        self.dfTime["validGap"] = self.dfTime["gapNext"] > min_gap_time

        self.dfTime["phantomGap"] = (
            self.dfTime["gapNext"] >= zero_gap_time
        ) & (self.dfTime["gapNext"] <= min_gap_time)

        del zero_gap_time, min_gap_time

        is_valid_row = self.dfTime["validGap"] + self.dfTime["phantomGap"]
        count_all = is_valid_row.count()
        total_valid = is_valid_row.sum()

        self.is_valid_data = count_all == total_valid

        if not (self.is_valid_data):
            print(
                "There are",
                count_all - total_valid,
                "'invalid' sequences of time within the data.\n This is not too unexpected, but will require some additional steps to resolve these overlapping time periods.",
            )
            return self.is_valid_data
        else:
            surviving_data = (
                len(self.dfTime.index) / self.inputDataSplitRowCount
            )
            surviving_data = round(surviving_data, 4) * 100

            print(
                f"""
Valid data: {surviving_data}% ({len(self.dfTime.index)} of {self.inputDataSplitRowCount}). This data is ready for analysis.
                  """
            )
            return self.is_valid_data

    def validateData(self):

        ### One step to run all the validation code, and a while loop to run as many iterations as required.

        i = 0
        self.calcGapLastGapNext()
        self.validateGapTiming()

        while not (self.is_valid_data):
            i += 1
            print("running validation loop step:", i)
            self.dfTime = self.dfTime[
                (self.dfTime["validGap"]) | (self.dfTime["phantomGap"])
            ]
            self.calcGapLastGapNext()
            self.validateGapTiming()

    ##
    def initializeAnalysis(self):
        print(
            "This will run the following",
            "1 - error if setVars() has not be done",
            "2 - run sortdata()",
            "3 - createDateVar()",
            "4 - calcGapLastGapNext(), validateGapNextGapLast()",
            "5 - based on step 3 know whether step3 should be repeated",
            "THEN move onto more substantive analysis elements",
        )

    ##
    ##
    ##
    ##
    ##
    ## Cluster Events (Shifts)
    def clusterEvents(self):

        self.dfTime["cluster_start"] = self.dfTime[self.tsIn].where(
            self.dfTime["gapLast"]
            >= pd.Timedelta(minutes=self.ResetTimerMinutes)
        )

        self.dfTime["cluster_start"] = self.dfTime.groupby(self.dataKey)[
            "cluster_start"
        ].fillna(method="ffill")

        self.dfTime["cluster_end"] = self.dfTime[self.tsOut].where(
            self.dfTime["gapNext"]
            >= pd.Timedelta(minutes=self.ResetTimerMinutes)
        )

        self.dfTime["cluster_end"] = self.dfTime.groupby(
            [self.dataKey, "cluster_start"]
        )["cluster_end"].fillna(method="bfill")


#%% Read in data

testData = pd.read_csv(
    "./Example Data/Simulated Timecard Data for 2 Employees.csv",
    dtype="str",
    parse_dates=["In.Actual.dt", "Out.Actual.dt"],
)

testData.rename(
    inplace=True,
    columns={
        "Person.ID": "EEID",
        "In.Actual.dt": "clock_in",
        "Out.Actual.dt": "clock_out",
    },
)

testData.head()


# %%

TCA = TimeClockAnalysis()
TCA.setVars(
    inputData=testData,
    IDKey="EEID",
    TimeStart="clock_in",
    TimeEnd="clock_out",
    ResetTimerMinutes=240,
)


#%%

TCA.splitAtMidnight()
TCA.createDateVar()
TCA.sortData()

### Commenting out for QA development
## test_preDrop = TCA.exportDataInProgress()

TCA.validateData()

test = TCA.exportDataInProgress()
##TODO: How do we declare certain gap lengths as phantom gaps? If (gap_next <= Seconds) then 'skip' the gap. This would allow for ending a day at '23:59' and the 1 second gap to midnight would not be 'valid' or count, but would help with QA.


#%%

test[
    (
        (test["EEID"] == "_000002")
        & (test["date"] >= parse("2020-03-01").date())
    )
]

#%%

TCA.clusterEvents()
test = TCA.exportDataInProgress()

# %%

test_subset = test[
    (
        (test["EEID"] == "_000002")
        & (test["date"] >= parse("2020-03-01").date())
    )
]

test_subset = test_subset[["EEID", "clock_in", "clock_out"]]


test_subset["elapsed_time_timedelta"] = (
    test_subset["clock_out"] - test_subset["clock_in"]
)


def timedelta_to_minutes(timedelta):
    days, seconds = timedelta.days, timedelta.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    return hours * 60 + minutes


test_subset["elapsed_time"] = test_subset["elapsed_time_timedelta"].apply(
    lambda x: timedelta_to_minutes(x)
)

test_subset

# %%
