import configparser
import os

CONFIG_FILE = 'config.ini'

def load_config():
    config = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Configuration file {CONFIG_FILE} not found.")
    
    # We need to add a dummy section header if the file doesn't have one, 
    # but the user's file looks like simple key-value pairs which might fail standard ConfigParser
    # without a section. Let's try to read it with a dummy section.
    
    with open(CONFIG_FILE, 'r') as f:
        config_string = '[DEFAULT]\n' + f.read()
    
    config.read_string(config_string)
    return config['DEFAULT']

if __name__ == "__main__":
    c = load_config()
    print(f"Loaded config for user: {c.get('username')}")
