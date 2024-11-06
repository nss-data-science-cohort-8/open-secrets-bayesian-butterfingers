from fuzzywuzzy import fuzz, process
import requests
import pandas as pd
from io import StringIO
import tqdm
from bs4 import BeautifulSoup
import re

def retrieve_2020_state_district_data(state: str, district: int) -> pd.DataFrame:
    """
    Processes the input state name and district number and returns a DataFrame for that district's 2020 election data.

    Args:
        state: the input State name or State abbreviation.
        district: the input District number.

    Returns:
        A District Election DataFrame.
    """

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
    #states_df is a dataframe that includes states and territories and their abbreviations
    state_abr_dict = states_df.set_index('State')['Abbreviation'].to_dict()
    #state_abr_dict is a dictionary that maps State names to their abbreviations

    states_list = state_abr_dict.keys()
    abbr_list = state_abr_dict.values()
    base_url = 'https://www.opensecrets.org/races/summary.csv?cycle=2020&id='
    district_num = f'{district:02}'
    if len(state) > 2:
        closest_match, score = process.extractOne(state, states_list, scorer=fuzz.ratio)
        if score < 100:
            print(f"No state by the name '{state}'. Assuming you meant '{closest_match}'.")
            state_district_url = base_url+state_abr_dict[closest_match]+district_num
        else:
            state_district_url = base_url+state+district_num
    elif len(state) < 2:
        raise ValueError(f"No state could be found under the name '{state}'. Please use a full abbreviation or state name.")
    else:
        closest_match, score = process.extractOne(state, abbr_list, scorer=fuzz.ratio)
        if score < 100:
            print(f"No state abbreviation by the name '{state}'. Assuming you meant '{closest_match}'.")
            state_district_url = base_url+closest_match+district_num
        else:
            state_district_url = base_url+state+district_num
    response = requests.get(state_district_url)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    state_distr_df = pd.read_csv(StringIO(response.text), sep=',')
    state_distr_df.insert(0, 'State_Abbreviation', closest_match if len(state)==2 else state_abr_dict[closest_match])
    state_distr_df.insert(1, 'District', district_num)
    state_distr_df.insert(4, 'Party', state_distr_df['FirstLastP'].apply(lambda x: re.sub(r'[()]', '', x.split()[-1])))
    state_distr_df['FirstLastP'] = state_distr_df['FirstLastP'].apply(lambda x: ' '.join(x.split()[:-1]))
    state_distr_df = state_distr_df.rename(columns={'FirstLastP':'FirstLast'})
    return state_distr_df

def get_all_data(states: list = None) -> pd.DataFrame:
    """
    Retrieve and compile every state's district 2020 election data.

    Parameters:
    ----------
    Args:
        states: a list of states to find all districts' data. Default None and will look at all 50 states.

    Returns:
    ----------
    pd.DataFrame
        All Election Data.
    """

    URL1 = 'https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations'
    states_df = pd.read_html(URL1)[1]
    states_df.columns = states_df.columns.map(lambda x: x[1])
    states_df = (
        states_df
        .reset_index(drop=True)
        .drop(columns=['Status of region', 'Unnamed: 2_level_1', 'Unnamed: 4_level_1',
                       'Unnamed: 5_level_1', 'Unnamed: 6_level_1', 'GPO', 'AP', 'Other abbreviations'])
        .dropna()
        .rename(columns={'Name': 'State', 'Unnamed: 3_level_1': 'Abbreviation'})
        .drop(0)
        .reset_index(drop=True)
    )

    URL2 = 'https://en.wikipedia.org/wiki/2020_United_States_House_of_Representatives_elections'
    response = requests.get(URL2)
    if response.status_code == requests.codes.ok:
        soup = BeautifulSoup(response.text, features="html.parser")
    else:
        response.raise_for_status()

    tables_html = str(soup.find_all('table', attrs={'class': 'wikitable'}))
    all_states_df = pd.read_html(StringIO(tables_html))[1].fillna('-')
    all_states_df.columns = all_states_df.columns.map(lambda x: x[1])
    all_states_df = all_states_df.drop(columns=['Seats', 'Change'])
    
    state_representatives_df = pd.merge(left=all_states_df, right=states_df, on='State', how='left')
    state_representatives_df = state_representatives_df.drop(state_representatives_df.index[-1])
    state_district_dict = state_representatives_df.set_index('Abbreviation')['Total seats'].to_dict()

    def get_closest_match(input_state, choices):
        match, score = process.extractOne(input_state, choices, scorer=fuzz.ratio)
        if score < 80:
            raise ValueError(f"No close match found for '{input_state}'. Please check your input.")
        return match

    data_list = []
    result_df = pd.DataFrame()

    if states:
        state_names = states_df['State'].tolist()
        state_abbreviations = states_df['Abbreviation'].tolist()

        for state in states:
            if len(state) > 2:
                closest_match = get_closest_match(state, state_names)
                abbreviation = states_df.loc[states_df['State'] == closest_match, 'Abbreviation'].values[0]
            else:
                closest_match = get_closest_match(state, state_abbreviations)
                abbreviation = closest_match

            for district in tqdm.tqdm(range(1, int(state_district_dict[abbreviation]) + 1)):
                data_list.append(retrieve_2020_state_district_data(abbreviation, district))
    else:
        for state in tqdm.tqdm(state_district_dict):
            for district in range(1, int(state_district_dict[state]) + 1):
                data_list.append(retrieve_2020_state_district_data(state, district))

    for data_df in data_list:
        result_df = pd.concat([result_df, data_df])

    return result_df
