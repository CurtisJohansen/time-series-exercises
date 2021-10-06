import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

import acquire
import requests
import os

from datetime import timedelta, datetime

def prep_items(df):
    '''
    This function takes in dataframe and drops unnecessary columns, adds a month, weekday and sales_total column
    '''
    # drop extra columns
    df.drop(columns=['Unnamed: 0_x', 'Unnamed: 0_y', 'Unnamed: 0'], inplace=True)

    # change date column to datetime
    df.sale_date = pd.to_datetime(df.sale_date)

    # change date to index
    df = df.set_index('sale_date').sort_index()

    # create month and column year
    df['month'] = df.index.month
    df['weekday'] = df.index.day_name()

    # create sales total column
    df['sales_total'] = df.sale_amount*df.item_price

    return df


def prep_germany(df):
    '''
    This function takes in a dataframe and sets the date as index, creates and month and year column,
    and fills all the nulls with the mean average of that column
    '''    
    # convert Date column to datetime
    df.Date = pd.to_datetime(df.Date)
    
    # set date column as index
    df = df.set_index('Date').sort_index()
    
    # make a month and year column
    df['month'] = df.index.month
    df['year'] = df.index.year
    
    # fill nulls with mean average

    df.fillna(0, inplace=True)
    
    return df


def split_time_series(df):
    '''
    This function takes in a dataframe and (based off time series) and returns a train and test df
    '''
    train_size = .70
    n = df.shape[0]
    test_start_index = round(train_size * n)

    train = df[:test_start_index] # everything up (not including) to the test_start_index
    test = df[test_start_index:] # everything from the test_start_index to the end

    return train, test