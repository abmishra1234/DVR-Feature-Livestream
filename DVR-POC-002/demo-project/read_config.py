import json

def read_config():
    with open('config.json', 'r') as file:
        config = json.load(file)
    return config['directory_path']
