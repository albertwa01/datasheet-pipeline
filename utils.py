import json
import urllib.parse


def create_connection_string_from_json(file_path):
    """
    Creates a PostgreSQL connection string using credentials from a JSON file.

    Args:
        file_path (str): Path to the JSON file containing credentials with keys:
            - 'user'
            - 'password'
            - 'host'
            - 'port'
            - 'database'

    Returns:
        str: A formatted PostgreSQL connection string.
    """
    # Load credentials from the JSON file
    with open(file_path, 'r') as file:
        credentials = json.load(file)

    # Extract and URL-encode the password to handle special characters
    username = credentials.get("user")
    password = urllib.parse.quote_plus(credentials.get("password"))
    host = credentials.get("host")
    port = credentials.get("port")
    database = credentials.get("database")

    return f"postgresql://{username}:{password}@{host}:{port}/{database}"

