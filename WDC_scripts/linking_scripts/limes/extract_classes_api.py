import requests
import os
from rdflib import Graph, URIRef, Literal, RDF, Namespace

OWL = Namespace("http://www.w3.org/2002/07/owl#")

api_url = "https://clinicaltrials.gov/api/v2/studies"
params = {"format": "json"}
page_token = None
page_number = 1
output_file = "clinical_trials_classes.nt"

checkpoint_file = "checkpoint.txt"
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        checkpoint_data = f.read().splitlines()
        page_number = int(checkpoint_data[0])
        page_token = checkpoint_data[1] if checkpoint_data[1] != None else None
    print(f"Resuming from page {page_number}")

with open(output_file, "a") as nt_file:
    while True:
        if page_token:
            params["pageToken"] = page_token
        else:
            params.pop("pageToken", None)

        response = requests.get(api_url, params=params)
        if response.status_code == 200:
            try:
                data = response.json()

                g = Graph()

                for study in data.get("studies", []):
                    nct_id = study.get("protocolSection", {}).get("identificationModule").get("nctId")
                    breif_title = study.get("protocolSection", {}).get("identificationModule", {}).get("briefTitle")

                    if nct_id and breif_title:
                        uri = URIRef(f"http://clinicaltrials.gov/study/{nct_id}")
                        g.add((uri, OWL.Class, Literal(breif_title)))

                nt_file.write(g.serialize(format="nt"))
                print(f"Appended page {page_number} to '{output_file}'")

                g.close()

                page_token = data.get("nextPageToken")
                with open(checkpoint_file, "w") as f:
                    f.write(f"{page_number + 1}\n{page_token}")

                page_number += 1

                if not page_token:
                    print("Reached the last page.")
                    break

            except requests.JSONDecodeError:
                print("Error: Unable to decode JSON response.")
                print("Response content:", response.text)
                exit()
        else:
            print(f"Error: Received status code {response.status_code}")
            print("Response content:", response.text)
            exit()

if os.path.exists(checkpoint_file):
    os.remove(checkpoint_file)
    print("Checkpoint cleared, all pages processed.")
else:
    print("No valid data to serialize.")
