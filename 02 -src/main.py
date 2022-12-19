import glob
import json
import uuid
from dateutil.parser import parse
from datetime import timedelta

def get_files() -> list:
    """
    This function grabs all the files that adhere to a specific string in the data directory. 
    The string it checks for is: training-sessions*.json
    """

    files = glob.glob('C:/Code/Training data/ETL-workoutdata/data/training-session*.json')

    files_read = len(files)
    print(f'{files_read} training sessions to extract data from')
    
    return files

def get_data(filepaths: list[str]) -> None:
    with open(filepaths[-1], 'r') as f:
        data = json.load(f)
    
    filename = filepaths[-1]

    unique_id = uuid.uuid4()

    original_uuid = filename[-41:]
    original_uuid = original_uuid.replace('.json', '')
    
    device_recorded_with = data['deviceId']

    raw_duration = data['duration']

    calories = data['kiloCalories']


def main():
    filepaths = get_files()

    get_data(filepaths)

if __name__ == '__main__':
    excersise_data = []
    related_exercises_data = []

    main()