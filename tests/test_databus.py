import pathlib
import tempfile

from pipeline import databus

EXAMPLE_ARTIFACT = "https://energy.databus.dbpedia.org/henhuy/OEP/rli_dibt_windzone"
EXAMPLE_COLLECTION = "https://energy.databus.dbpedia.org/henhuy/collections/sedos_test"
CSV_ARTIFACT = "https://energy.databus.dbpedia.org/henhuy/general/testartifact/2022-01-20/testartifact_type=randomData.csv"


def test_version():
    version = databus.get_latest_version_of_artifact(EXAMPLE_ARTIFACT)
    assert version == "2022-06-24"


def test_collections():
    artifacts = databus.get_artifacts_from_collection(EXAMPLE_COLLECTION)
    assert len(artifacts) == 2
    assert (
        artifacts[0]
        == "https://energy.databus.dbpedia.org/henhuy/OEP/rli_dibt_windzone"
    )
    assert (
        artifacts[1] == "https://energy.databus.dbpedia.org/henhuy/general/testartifact"
    )


def test_artifact_file():
    databus_file = databus.get_artifact_file(EXAMPLE_ARTIFACT, "2022-06-24")
    assert (
        databus_file
        == "https://energy.databus.dbpedia.org/henhuy/OEP/rli_dibt_windzone/2022-06-24/rli_dibt_windzone_.json"
    )


def test_download_csv():
    test_filename = "test.csv"
    with tempfile.TemporaryDirectory() as tmpdirname:
        csv_filename = pathlib.Path(tmpdirname) / test_filename
        assert not csv_filename.exists()
        databus.download_artifact(CSV_ARTIFACT, csv_filename)
        assert csv_filename.exists()
