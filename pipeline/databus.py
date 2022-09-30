import json
import os
import pathlib
import urllib.parse
from typing import List, Union

import requests
from SPARQLWrapper import JSON, SPARQLWrapper2

DATABUS_ENDPOINT = "https://energy.databus.dbpedia.org/sparql"


def download_artifact(artifact_file: str, filename: Union[pathlib.Path, str]):
    """
    Downloads a CSV artifact and stores it at given filename

    Parameters
    ----------
    artifact_file: str
        URI to artifact file
    filename: str
        Path to store downladed file

    Raises
    ------
    NotImplementedError
        If artifact file is not a CSV file
    """
    if not artifact_file.endswith(".csv"):
        raise NotImplementedError("Currently only CSV artifacts can be downloaded.")
    with open(os.path.join(filename), "wb") as f, requests.get(
        artifact_file, stream=True, timeout=90
    ) as r:
        for line in r.iter_lines():
            f.write(line + "\n".encode())


def get_artifact_file(artifact: str, version: str) -> str:
    sparql = SPARQLWrapper2(DATABUS_ENDPOINT)
    sparql.setReturnFormat(JSON)

    sparql.setQuery(
        f"""
        PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dcat:   <http://www.w3.org/ns/dcat#>
        PREFIX dct:    <http://purl.org/dc/terms/>
        PREFIX dcv: <http://dataid.dbpedia.org/ns/cv#>
        PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
        SELECT ?file WHERE
        {{
            GRAPH ?g
            {{
                ?dataset dataid:artifact <{artifact}> .
                ?distribution <http://purl.org/dc/terms/hasVersion> '{version}' .
                ?distribution dataid:file ?file .
            }}
        }}
        """
    )
    result = sparql.query()
    return result.bindings[0]["file"].value


def get_latest_version_of_artifact(artifact: str) -> str:
    """
    Returns the latest version of given artifact

    Parameters
    ----------
    artifact: str
        DataId of artifact to check version of

    Returns
    -------
    str
        Latest version of given artifact
    """
    sparql = SPARQLWrapper2(DATABUS_ENDPOINT)
    sparql.setReturnFormat(JSON)

    sparql.setQuery(
        f"""
        PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX dcat:   <http://www.w3.org/ns/dcat#>
        PREFIX dct:    <http://purl.org/dc/terms/>
        PREFIX dcv: <http://dataid.dbpedia.org/ns/cv#>
        PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
        SELECT ?version WHERE
        {{
            GRAPH ?g
            {{
                ?dataset dataid:artifact <{artifact}> .
                ?dataset dct:hasVersion ?version .
            }}
        }} ORDER BY DESC (?version) LIMIT 1
        """
    )
    result = sparql.query()
    return result.bindings[0]["version"].value


def get_artifacts_from_collection(collection: str) -> List[str]:
    """
    Returns list of all artifacts found in given collection

    Parameters
    ----------
    collection: str
        URL to databus collection

    Returns
    -------
    List[str]
        List of artifacts in collection
    """

    def find_artifact(node):
        if len(node["childNodes"]) == 0:
            yield node["uri"]
        for child in node["childNodes"]:
            yield from find_artifact(child)

    response = requests.get(
        collection, headers={"Content-Type": "text/sparql"}, timeout=90
    )
    data = response.json()
    content_raw = urllib.parse.unquote(data["@graph"][0]["content"])
    content = json.loads(content_raw)
    return list(find_artifact(content["root"]))
