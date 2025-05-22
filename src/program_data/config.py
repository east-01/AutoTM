import os
import yaml

def load_config():
    config_file = "./config.yaml"

    if(not os.path.isfile(config_file)):
        print(f"Error: The config file \"{config}\" doesn't exist. Exiting...")
        exit(1)

    try:
        # Load the YAML file
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        # Ensure the YAML was parsed correctly
        if not isinstance(config, dict):
            raise ValueError("Invalid YAML format. Expected a dictionary structure.")

        return config

    except yaml.YAMLError as e:
        # Catch and handle YAML syntax errors
        print(f"Error: YAML syntax issue in the config file. Details: {e}")
    except KeyError as e:
        # Handle missing keys in the YAML file
        print(f"Error: Missing expected key in the config file: {e}")
    except ValueError as e:
        # Handle any value errors
        print(f"Error: {e}")

import json

def verify_config(prog_data):
    if(prog_data.config is None):
        print("Failed to load configuration. Exiting.")
        exit(1)

    # A list of keys to ensure they: exist in config, have a value
    keys_to_check = ["base_url", "queries", "step"]

    for key_to_check in keys_to_check:
        if(key_to_check not in prog_data.config.keys() or len(str(prog_data.config[key_to_check])) == 0):
            print(f"Failed to load configuration. Key \"{key_to_check}\" either doesn't exist in config or has no value. Exiting.")
            exit(1)

    if(set(prog_data.config["queries"].keys()) != set(["status", "truth"])):
        print(f"Failed to load configuration. \"Queries\" section expects subsections \"status\" and \"truth\"")
        exit(1)

    if(prog_data.settings['type_string_identifier'] not in prog_data.config["queries"]["truth"]):
        print(f"The truth query (as specified in the configuration) doesn't have the type string identifier \"{prog_data.settings['type_string_identifier']}\" in it. Exiting.")
        exit(1)

    return