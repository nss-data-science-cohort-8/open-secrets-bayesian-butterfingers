from fuzzywuzzy import fuzz, process
import requests
import pandas as pd
from io import StringIO
import tqdm
from bs4 import BeautifulSoup
import re

#Global variables to store information
state_district_dict = None
states_df = None
state_to_abr_dict = None
abr_to_state_dict = None
state_representatives_df = None

def setup_state_data():
    global state_district_dict, states_df, state_to_abr_dict, abr_to_state_dict, state_representatives_df

    if state_district_dict is None or states_df is None or state_to_abr_dict is None or abr_to_state_dict is None:
        #Retrieve and parse state abbreviation data
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
        )

        #Retrieve and parse state district (number of seats) data
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
        
        #Create dictionaries mapping state names to their abbreviations and vice versa
        state_to_abr_dict = state_representatives_df.set_index('State')['Abbreviation'].to_dict()
        abr_to_state_dict = state_representatives_df.set_index('Abbreviation')['State'].to_dict()

        #Create dictionary mapping state abbreviaton to the total number of seats/districts for that state
        state_district_dict = state_representatives_df.set_index('Abbreviation')['Total seats'].to_dict()

    #print("All state data is now set up.")

def get_num_seats(state: str) -> int:
    setup_state_data()
    if state in state_district_dict:
        return int(state_district_dict[state])
    else:
        raise ValueError(f"No data found for state '{state}'. Please use a valid state abbreviation.")

def retrieve_2020_state_district_data(state: str, district: int) -> pd.DataFrame:
    """
    Retrieve 2020 election data for a specified state and district from OpenSecrets.

    This function constructs a URL for fetching election data from the OpenSecrets website 
    using the provided state name or abbreviation and district number. If the input state 
    name or abbreviation is not an exact match, fuzzy matching is used to suggest the closest 
    valid option. The retrieved data is processed and returned as a DataFrame, with additional 
    processing to clean up names and extract party affiliations.

    Args:
        state (str): The name or abbreviation of the U.S. state. If a name is provided, 
                     fuzzy matching will attempt to find the closest match if necessary.
        district (int): The district number (an integer) for which to retrieve election data. 
                        The number is zero-padded to ensure it is a two-digit string.

    Returns:
        pd.DataFrame: A DataFrame containing the election data for the specified state and district.
                      The DataFrame includes columns such as 'State_Abbreviation', 'District', 
                      'FirstLast', and 'Party', with cleaned up names and party affiliations.

    Raises:
        ValueError: If the provided state name or abbreviation is invalid, or if the district number
                    exceeds the number of districts for the given state.
        requests.exceptions.RequestException: If there is an error while making the HTTP request
                                              to fetch the election data.
    """
    if district < 1:
        raise ValueError('District numbers must be greater than 0.')

    setup_state_data()

    states_list = state_to_abr_dict.keys()
    abbr_list = state_to_abr_dict.values()
    base_url = 'https://www.opensecrets.org/races/summary.csv?cycle=2020&id='
    district_num = f'{district:02}'

    if len(state) > 2:  #Full State Name
        closest_match, score = process.extractOne(state, states_list, scorer=fuzz.ratio)
        num_seats = get_num_seats(state_to_abr_dict[closest_match])
        
        if district > num_seats:
            if num_seats > 1:
                raise ValueError(f"'{closest_match}' only has {num_seats} districts.")
            else:
                raise ValueError(f"'{closest_match}' only has {num_seats} district.")
        
        if score < 100:
            print(f"No state by the name '{state}'. Assuming you meant '{closest_match}'.")
            state_district_url = base_url + state_to_abr_dict[closest_match] + district_num
        else:
            state_district_url = base_url + state + district_num

    elif len(state) < 2:
        raise ValueError(f"No state could be found under the name '{state}'. Please use a full abbreviation or state name.")

    else:  #State Abbreviation
        closest_match, score = process.extractOne(state, abbr_list, scorer=fuzz.ratio)
        num_seats = get_num_seats(closest_match)
        
        if district > num_seats:
            if num_seats > 1:
                raise ValueError(f"'{abr_to_state_dict[closest_match]}' only has {num_seats} districts.")
            else:
                raise ValueError(f"'{abr_to_state_dict[closest_match]}' only has {num_seats} district.")
        
        if score < 100:
            print(f"No state abbreviation by the name '{state}'. Assuming you meant '{closest_match}'.")
            state_district_url = base_url + closest_match + district_num
        else:
            state_district_url = base_url + state + district_num


    #Fetch district data
    response = requests.get(state_district_url)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    state_distr_df = pd.read_csv(StringIO(response.text), sep=',')

    #Update the DataFrame
    state_distr_df.insert(0, 'State_Abbreviation', closest_match if len(state) == 2 else state_to_abr_dict[closest_match])
    state_distr_df.insert(1, 'District', district_num)
    state_distr_df.insert(4, 'Party', state_distr_df['FirstLastP'].apply(lambda x: re.sub(r'[()]', '', x.split()[-1])))
    state_distr_df['FirstLastP'] = state_distr_df['FirstLastP'].apply(lambda x: ' '.join(x.split()[:-1]))
    state_distr_df = state_distr_df.rename(columns={'FirstLastP': 'FirstLast'})

    return state_distr_df

def get_all_data(states: list = None) -> pd.DataFrame:
    """
    Retrieve and compile 2020 election data for all U.S. states and their districts.

    This function fetches election data for all U.S. states and their districts from OpenSecrets.
    It processes the data for each district in the given states or all states if no specific states 
    are provided. The data is compiled into a single DataFrame and returned.

    Args:
        states (list, optional): A list of state names or abbreviations. If None, data for all 50 
                                  states is retrieved. The states can be provided either as full 
                                  state names or abbreviations.

    Returns:
        pd.DataFrame: A DataFrame containing the election data for all districts in the provided 
                      states or for all states if no specific states are provided. The DataFrame 
                      contains columns such as 'State_Abbreviation', 'District', 'FirstLast', 
                      and 'Party'.

    Raises:
        ValueError: If no close match is found for a state name or abbreviation provided in the list.
        requests.exceptions.RequestException: If there is an error during the retrieval of data for 
                                               any state or district.
    """
    setup_state_data()

    def get_closest_match(input_state, choices):
        match, score = process.extractOne(input_state, choices, scorer=fuzz.ratio)
        if score < 80:
            raise ValueError(f"No close match found for '{input_state}'. Please check your input.")
        return match

    data_list = []

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

            for district in tqdm.tqdm(range(1, get_num_seats(abbreviation) + 1)):
                data_list.append(retrieve_2020_state_district_data(abbreviation, district))
    else:
        for state in tqdm.tqdm(state_district_dict):
            for district in range(1, get_num_seats(state) + 1):
                data_list.append(retrieve_2020_state_district_data(state, district))

    result_df = pd.concat(data_list, ignore_index=True)

    return result_df