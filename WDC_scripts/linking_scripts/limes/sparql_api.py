import requests
import time
import urllib.parse

def send_sparql(sparql_endpoint, query, max_retries=10, initial_wait_time=5):
    """
    Sends a SPARQL query to the specified endpoint with retries and exponential backoff.
    
    :param sparql_endpoint: The SPARQL endpoint URL.
    :param query: The SPARQL query string.
    :param max_retries: Maximum number of retries if the request fails (default is 10).
    :param initial_wait_time: Initial wait time in seconds before retrying (default is 5 seconds).
    :return: The result of the query in json, or False in case of failure.
    """
    query = query.strip()
    encoded_query = urllib.parse.quote(query, safe=":/")
    full_url = f"{sparql_endpoint}?query={encoded_query}"
    print(full_url)
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(full_url)
            response.raise_for_status()

            return response.text

        except requests.exceptions.RequestException as e:
            print(f"Attempts {attempt} failed with error: {e}")
            if attempt < max_retries:
                wait_time = initial_wait_time * (2 ** (attempt - 1))
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Skipping this query.")
                return None
