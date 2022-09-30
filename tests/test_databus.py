from pipeline import databus


def test_version():
    EXAMPLE_ARTIFACT = "https://energy.databus.dbpedia.org/henhuy/OEP/rli_dibt_windzone"
    version = databus.get_latest_version_of_artifact(EXAMPLE_ARTIFACT)
    assert version == "2022-06-24"


def test_collections():
    EXAMPLE_COLLECTION = (
        "https://energy.databus.dbpedia.org/henhuy/collections/sedos_test"
    )
    artifacts = databus.get_artifacts_from_collection(EXAMPLE_COLLECTION)
    assert len(artifacts) == 2
    assert (
        artifacts[0]
        == "https://energy.databus.dbpedia.org/henhuy/OEP/rli_dibt_windzone"
    )
    assert (
        artifacts[1] == "https://energy.databus.dbpedia.org/henhuy/general/testartifact"
    )
