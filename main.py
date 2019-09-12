import pandas as pd
import numpy as np
import datetime

# -----------------------
# define globals
# -----------------------

# tariffs are defined as p/kWh 
# FLAT_RATE tariff is applied to all time ranges of a day wihin a month 
# the alternative of economy 7, applies the tariff ECONOMY for the time 
# range 23:00 to 6:00 and the FLAT_RATE tariff for the remaining hours within one day

ECONOMY = 0.11
FLAT_RATE = 0.15

# -------------------------------------------
# Filters
# -------------------------------------------


def get_meter_data(df, meter_id):
    """
    filters the smart meter readings in the dataframe by smart meterID,

    queries values from the column "meter_id", 

    returns a new dataframe (i.e. filtered_df)
    """
    filtered_df = df.query('meter_id == "' + meter_id + '"')

    return filtered_df


def get_monthly_readings(df, year, month):
    """
    extracts the month and year separately from the "DateTime" column

    df holds the new dataframe

    returns new dataframe (i.e. df)

    """
    # filter by year
    df = df[
        df['DateTime'].map(lambda x: x.year) ==
        year
    ]

    # filter by month
    df = df[
        df['DateTime'].map(lambda x: x.month) ==
        month
    ]

    return df


def get_time_range_readings(df, startTime, endTime):
    """
    selects values between a particular time range,this is necessary to define

    the timecranges during which Economy7 tariff will be applied,

    converts the "DateTime" column into a DateTimeIndex

    returns a dataframe

    """

    df.index = df['DateTime']
    df = df.between_time(
        startTime,
        endTime,
    )

    return df

# -------------------------------------------
# Sanitizing functions
# -------------------------------------------


def remove_negative_readings(df):
    """
    selects all non-negative readings of the "consumption_kwh" column,

    df_filtered holds the new dataframe containing only positive numbers

    returns the a new dataframe 

    """

    df_filtered = df[df['consumption_kwh'].map(lambda x: x >= 0)]
    return df_filtered


def remove_duplicates(df):
    """
    drops a row containing duplicate readings 

    df_cleaned holds the dataframe without duplicates

    returns the a new dataframe

    """

    df_cleaned = df.drop_duplicates(keep='first')
    return df_cleaned

# -------------------------------------------
# computing new values
# -------------------------------------------


def compute_current_cost(cost, readings):
    """
    uses the filtered dataframe

    sums up the energy consumption,
    
    multiplies by 0.15p/kWh and converts to £

    considers the costs when applying the FLAT_RATE tariff
    """

    return (np.sum(readings) * cost) * 100


def compute_costs(df):
    """
    creates new df and assigns new columns

    cost_df holds the new df

    for a unique meter_id the monthly consumption is summed up

    the for loop allows to select specific timeranges (night_readings and

    day_readings) for a specific month

    """

    cost_df = pd.DataFrame(
        columns=['meter_id', 'month', 'year', 'cost(£)', 'saved cost(£)'],
    )

    meter_id_series = df.meter_id.unique()

    for meter_id in meter_id_series:
        # apply filters
        # filter by meterID, input a smart meter ID
        meter_df = get_meter_data(df, meter_id)
        # filter by month
        for month in range(1, 13):

            
            # obtain monthly readings for a unique meterID and given teh year 2013
            month_readings = get_monthly_readings(meter_df, 2013, month)

            # if there's no data for this timepoint, skip
            if month_readings.empty:
                continue

            # sum all meter readings in the filtered data
            month_consumption = month_readings['consumption_kwh'].sum()

            # assume the economy 7 tariff is applied daily for seven hours from 11pm to 6am
            night_readings = get_time_range_readings(
                month_readings,
                '23:00',
                '06:00',
            )
            day_readings = get_time_range_readings(
                month_readings,
                '06:00',
                '23:00',
            )

            # calculate the cost based on current tariff
            currentCost = compute_current_cost(FLAT_RATE, month_consumption)

            # calculate the cost with econmy7 tariff applied for seven hours
            economyCost = compute_current_cost(FLAT_RATE, day_readings['consumption_kwh'].sum(
            )) + compute_current_cost(ECONOMY, night_readings['consumption_kwh'].sum())

            # calculate the costs saved
            savedCost = currentCost - economyCost

            # add the values to the row
            row = (
                meter_id,
                month,
                2013,
                np.around(currentCost, decimals=2),
                np.around(savedCost, decimals=2),
            )
            # efficiently append the row to the new df
            cost_df.loc[-1] = row
            cost_df.index = cost_df.index + 1
            cost_df = cost_df.sort_index()

    return cost_df


def main():

    # Load the dataframe
    df = pd.read_csv(
        'ee_coding_challenge_dataset.csv',
        parse_dates=['DateTime'],
        infer_datetime_format=True,
        header=0,
    )

    # sanitizing input
    df = remove_negative_readings(df)
    df = remove_duplicates(df)

    cost_df = compute_costs(df)
    print(cost_df.head())

    # save outputs to external csv file
    cost_df.to_csv('output.csv', index=False)


main()