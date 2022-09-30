from pipeline import databus


def test_version():
    EXAMPLE_ARTIFACT = "https://energy.databus.dbpedia.org/henhuy/OEP/rli_dibt_windzone"
    version = databus.get_latest_version_of_artifact(EXAMPLE_ARTIFACT)
    assert version == "2022-06-24"
