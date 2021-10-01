#################### IMPORTS ####################

import pandas as pd 
import numpy as np

import requests 
import os


######################## ACQUIRE FUNCTIONS #################################

#################### GERMANY ENERGY FUNCTION #####################

def get_germany():
    
    '''
    This function creates a csv of germany energy data if one does not exist
    if one already exists, it uses the existing csv 
    and brings it into pandas as dataframe
    '''
    
    if os.path.isfile('opsd_germany_daily.csv'):
        df = pd.read_csv('opsd_germany_daily.csv', index_col=0)
        
    else:
        url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
        df = pd.read_csv(url)
        df.to_csv('opsd_germany_daily.csv')
        
    return df

############################# ACQUIRE DATA FUNCTION #########################

def get_df(name):
    
    """
    This function takes in the string 'items', 'stores', or 'sales' and
    returns a df containing all pages and creates a .csv file for future use.
    """
    
    base_url = 'https://python.zgulde.net'
    api_url = base_url + '/api/v1/'
    response = requests.get(api_url + name)
    data = response.json()

    file_name=(name+'.csv')

    if os.path.isfile(file_name):
        return pd.read_csv(name+'.csv')
    else:
        # create list from 1st page
        my_list = data['payload'][name]
        
        # loop through the pages and add to list
        while data['payload']['next_page'] != None:
            response = requests.get(base_url + data['payload']['next_page'])
            data = response.json()
            my_list.extend(data['payload'][name])
        
        # Create DataFrame from list
        df = pd.DataFrame(my_list)
        
        # Write DataFrame to csv file for future use
        df.to_csv(name + '.csv')

    return df


############################# MERGE DATA FUNCTION #########################

def combine_df(items, sales, stores):
    
    '''
    This functions takes in the three dataframes, items, sales, and stores and merges them.
    '''
    
    # rename columns to have a primary key
    items.rename(columns={'item_id':'item'}, inplace=True)
    stores.rename(columns={'store_id':'store'}, inplace=True)
    
    # merge the dataframes together
    items_sales = items.merge(sales, how='right', on='item')
    df = items_sales.merge(stores, how='left', on='store')
    
    return df

