import requests
import pandas as pd
import tqdm
from io import StringIO
from fuzzywuzzy import fuzz, process


def ensure_two_digits(num):
    # Ensures an integer is represented by two digits, padding with '0' if necessary.
    return str(num).zfill(2)


def retrieve_2020_state_district_data(state: str, district: int) -> pd.DataFrame:
    """
    Process the input state name and district number and returns a DataFrame for that district's 2020 election data.

    Args:
        state: the input State name or State abbreviation.
        district: the input District number.

    Returns:
        A District Election DataFrame.
    """

    URL = "https://en.wikipedia.org/wiki/List_of_U.S._state_and_territory_abbreviations"
    states_df = pd.read_html(URL)[1]
    states_df.columns = states_df.columns.map(lambda x: x[1])
    states_df = (
        states_df.reset_index()
        .drop(
            columns=[
                "index",
                "Status of region",
                "Unnamed: 2_level_1",
                "Unnamed: 4_level_1",
                "Unnamed: 5_level_1",
                "Unnamed: 6_level_1",
                "GPO",
                "AP",
                "Other abbreviations",
            ]
        )
        .dropna()
        .rename(columns={"Name": "State", "Unnamed: 3_level_1": "Abbreviation"})
        .drop(0)
        .reset_index(drop=True)
    )
    # states_df is a dataframe that includes states and territories and their abbreviations
    state_abr_dict = states_df.set_index("State")["Abbreviation"].to_dict()
    # state_abr_dict is a dictionary that maps State names to their abbreviations

    states_list = state_abr_dict.keys()
    abbr_list = state_abr_dict.values()
    base_url = "https://www.opensecrets.org/races/summary.csv?cycle=2020&id="
    district_num = f"{district:02}"
    if len(state) > 2:
        closest_match, score = process.extractOne(state, states_list, scorer=fuzz.ratio)
        if score < 100:
            print(f"No state by this name. Assuming you meant {closest_match}.")
            state_district_url = base_url + state_abr_dict[closest_match] + district_num
        else:
            state_district_url = base_url + state + district_num
    elif len(state) < 2:
        raise ValueError(
            f"No state could be found under the name {state}. Please use a full abbreviation or state name."
        )
    else:
        closest_match, score = process.extractOne(state, abbr_list, scorer=fuzz.ratio)
        if score < 100:
            print(
                f"No state abbreviation by this name. Assuming you meant {closest_match}."
            )
            state_district_url = base_url + closest_match + district_num
        else:
            state_district_url = base_url + state + district_num
    response = requests.get(state_district_url)
    if response.status_code != requests.codes.ok:
        response.raise_for_status()
    state_distr_df = pd.read_csv(StringIO(response.text), sep=",")
    state_distr_df.insert(
        0,
        "State_Abbreviation",
        closest_match if len(state) == 2 else state_abr_dict[closest_match],
    )
    state_distr_df.insert(1, "District", district_num)
    return state_distr_df


def get_all_data(state_representatives_df):
    # Tennessee       TN   7
    # Massachusetts   MA   6
    state_district_dict = state_representatives_df.set_index("Abbreviation")[
        "Total seats"
    ].to_dict()
    data_list = []
    result_df = pd.DataFrame()
    for state in tqdm.tqdm(state_district_dict):
        for district in range(1, state_district_dict[state] + 1):
            data_list.append(
                retrieve_2020_state_district_data(
                    state, district, state_abbreviations=True
                )
            )
    for data_df in data_list:
        result_df = pd.concat([result_df, data_df])
    return result_df
