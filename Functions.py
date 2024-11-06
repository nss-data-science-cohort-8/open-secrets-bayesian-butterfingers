import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import tqdm
from fuzzywuzzy import fuzz, process


def get_states_abr_df(URL:str):
    URL = 'https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations'
    states_df = pd.read_html(URL)[1]
    states_df.columns = states_df.columns.map(lambda x: x[1])
    states_df = (
        states_df
        .reset_index()
        .drop(columns = ['index', 'Status of region', 'Unnamed: 2_level_1', 'Unnamed: 4_level_1', 'Unnamed: 5_level_1', 'Unnamed: 6_level_1', 'GPO', 'AP', 'Other abbreviations'])
        .dropna()
        .rename(columns = {'Name': 'State', 'Unnamed: 3_level_1': 'Abbreviation'})
        .drop(0).reset_index(drop=True)
    )
    return states_df

def get_states_seats_df(URL:str):
    URL = 'https://en.wikipedia.org/wiki/2020_United_States_House_of_Representatives_elections'
    response = requests.get(URL)
    if response.status_code == requests.codes.ok:
        soup = BeautifulSoup(response.text, features="html.parser")
    else:
        response.raise_for_status()
    tables_html = str(soup.find_all('table', attrs={'class' : 'wikitable'}))
    all_states_df = pd.read_html(StringIO(str(tables_html)))[1].fillna('-')
    all_states_df.columns = all_states_df.columns.map(lambda x: x[1])
    all_states_df = all_states_df.drop(columns=['Seats', 'Change'])
    return all_states_df

def get_tn_dist7(URL:str):
    response = requests.get(URL)
    print(type(response))
    if response.status_code == requests.codes.ok:
        print('Request is okay!')
    else:
        response.raise_for_status()
    TN_district7 = pd.read_csv(StringIO(response.text), sep=',')
    return TN_district7

def create_state_rep_df(seats_df,abr_df):
    state_representatives_df = pd.merge(left=seats_df, right=abr_df, on='State')
    return state_representatives_df

def create_state_abr_dict(abr_df):

    states_abr_dict = abr_df.set_index('State')['Abbreviation'].to_dict()
    return states_abr_dict

def ensure_two_digits(num):
    #Ensures an integer is represented by two digits, padding with '0' if necessary.
    return str(num).zfill(2)

"""
def retrieve_2020_state_district_data(state: str, district: int, states_abr_dict: dict, state_abbreviations=False):
    #need some kind of dictionary that will take state name if state_abbreviations=False
    state_abr_dict = states_abr_dict
    base_url = 'https://www.opensecrets.org/races/summary.csv?cycle=2020&id='
    district_num = ensure_two_digits(district)
    if state_abbreviations:
        state_district_url = base_url+state+district_num
    else:
        state_district_url = base_url+state_abr_dict[state]+district_num
    response = requests.get(state_district_url)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
        #print('Check to see if state has this District number')
    state_distr_df = pd.read_csv(StringIO(response.text), sep=',')
    state_distr_df.insert(0, 'State_Abbreviation', state if state_abbreviations else state_abr_dict[state])
    state_distr_df.insert(1, 'District', district_num)
    return state_distr_df
"""

def retrieve_2020_state_district_data(state: str, district: int, state_abr_dict: dict) -> pd.DataFrame:
    """
    Process the input state name and district number and returns a DataFrame for that district's 2020 election data.

    Args:
        state: the input State name or State abbreviation.
        district: the input District number.

    Returns:
        A District Election DataFrame.
    """


    
    #state_abr_dict is a dictionary that maps State names to their abbreviations

    states_list = state_abr_dict.keys()
    abbr_list = state_abr_dict.values()
    base_url = 'https://www.opensecrets.org/races/summary.csv?cycle=2020&id='
    district_num = f'{district:02}'
    if len(state) > 2:
        closest_match, score = process.extractOne(state, states_list, scorer=fuzz.ratio)
        if score < 100:
            print(f'No state by this name. Assuming you meant {closest_match}.')
            state_district_url = base_url+state_abr_dict[closest_match]+district_num
        else:
            state_district_url = base_url+state+district_num
    elif len(state) < 2:
        raise ValueError(f'No state could be found under the name {state}. Please use a full abbreviation or state name.')
    else:
        closest_match, score = process.extractOne(state, abbr_list, scorer=fuzz.ratio)
        if score < 100:
            print(f'No state abbreviation by this name. Assuming you meant {closest_match}.')
            state_district_url = base_url+closest_match+district_num
        else:
            state_district_url = base_url+state+district_num
    response = requests.get(state_district_url)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    state_distr_df = pd.read_csv(StringIO(response.text), sep=',')
    state_distr_df.insert(0, 'State_Abbreviation', closest_match if len(state)==2 else state_abr_dict[closest_match])
    state_distr_df.insert(1, 'District', district_num)
    return state_distr_df


def get_all_data(state_representatives_df: dict, states_abr_dict: dict):
    #Tennessee       TN   7
    #Massachusetts   MA   6
    state_district_dict = state_representatives_df.set_index('Abbreviation')['Total seats'].to_dict()
    data_list = []
    result_df = pd.DataFrame()
    for state in tqdm.tqdm(state_district_dict):
        for district in range(1, state_district_dict[state]+1):
            data_list.append(retrieve_2020_state_district_data(state, district, states_abr_dict))
    for data_df in data_list:
        result_df = pd.concat([result_df, data_df])
    return result_df


