import glob
import json
import uuid
import pandas as pd
from dateutil.parser import parse
from datetime import timedelta

def get_data_from_json(dictionary: dict, given_key: str) -> str:
    """
    This function handles exceptions that may occur when grabbing the data from Json files

    Args:
        dictionary (dict): This argument is the json file that has the data
        given_key (str): This argument specifies the key in the dictionary you are trying to grab
    """
    try:
        data = dictionary[given_key]
        return data
    except KeyError:
        return None
    except TypeError:
        print(f'Something wrong with getting the data from this key... {given_key}')
        return None

def get_files() -> list:
    """
    This function grabs all the files that adhere to a specific string in the data directory. 
    The string it checks for is: training-sessions*.json
    """

    files = glob.glob('C:/Code/Training data/ETL-workoutdata/09-data/training-session*.json')

    files_read = len(files)

    print("")
    print(f'{files_read} training sessions to extract data from')
    print("")
    
    return files

def get_granular_exercise_data(exercise_id: str, json_data: dict) -> None:
    """
    This function should get all the granular data from an recorded excersise session. For instance it will return heartrate data (if recorded), distance (if recorded), etc.

    Args:
        exercise_id (str): This argument takes a randomly generated UUID. It's used to later beable to link certain excersises togheter
        json_data (dict): This argument takes a dictionary of json data to extract heart rate/distance/etc from
    """
    granular_data = json_data['exercises'][0]['samples']

    for key in granular_data.keys():
        if key == 'rr':
            continue
        else:
            each_keys_data = granular_data[key]
            for data in each_keys_data:
                type_of_measurement = key
                date_time_of_measurement = get_data_from_json(data, 'dateTime')
                value_of_measurement = get_data_from_json(data, 'value')
                longitude_of_measurement = get_data_from_json(data, 'longitude')
                latitude_of_measurement = get_data_from_json(data, 'latitude')
                altitude_of_measurement = get_data_from_json(data, 'altitude')

                data_to_append = [
                    exercise_id,
                    type_of_measurement,
                    date_time_of_measurement,
                    value_of_measurement,
                    longitude_of_measurement,
                    latitude_of_measurement,
                    altitude_of_measurement
                ]

                related_exercises_data.append(data_to_append)

def get_data(filepaths: list[str]) -> None:
    #initializing counter so we see how many files we have left to go
    x = 0 

    for file in filepaths:
        x += 1

        with open(file, 'r') as f:
            json_file_data = json.load(f)
        
        filename = file

        unique_id = uuid.uuid4()
        id_str = str(unique_id)

        original_uuid = filename[-41:]
        original_uuid = original_uuid.replace('.json', '')
        
        device_recorded_with = get_data_from_json(json_file_data, 'devideId')

        raw_duration = get_data_from_json(json_file_data, 'duration')

        total_calories = get_data_from_json(json_file_data, 'kiloCalories')

        start_time_recording = json_file_data['exercises'][0]['startTime']

        stop_time_recording = json_file_data['exercises'][0]['stopTime']

        sport_type = json_file_data['exercises'][0]['sport']

        data = [
            id_str, 
            original_uuid, 
            device_recorded_with, 
            raw_duration, 
            total_calories, 
            start_time_recording, 
            stop_time_recording,
            sport_type 
        ]

        excersise_data.append(data)

        get_granular_exercise_data(id_str, json_file_data)

        print(f'Getting data from file number {x} from file: {filename}')

def main():
    filepaths = get_files()

    get_data(filepaths)

if __name__ == '__main__':
    excersise_data = [['ID', 'original_UUID', 'Device_ID', 'Duration_exercise', 'Total Calories', 'Start_time', 'Stop_time', 'Sport']]
    related_exercises_data = [['Exercise_ID', 'Type_of_measurement', 'Datetime', 'Value_of_measurement', 'longitude_of_measurement','latitude_of_measurement', 'altitude_of_measurement']]

    main()

    exercise_df = pd.DataFrame(excersise_data)
    related_exercises_data_df = pd.DataFrame(related_exercises_data)

    exercise_df.columns = exercise_df.iloc[0]
    related_exercises_data_df.columns = related_exercises_data_df.iloc[0]

    exercise_df = exercise_df[1:]
    related_exercises_data_df = related_exercises_data_df[1:]

    exercise_df.to_csv('test1.csv')
    related_exercises_data_df.to_csv('test2.csv')