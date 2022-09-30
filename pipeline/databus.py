from SPARQLWrapper import JSON, SPARQLWrapper2

DATABUS_ENDPOINT = "https://energy.databus.dbpedia.org/sparql"


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
