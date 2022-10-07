import json
import pathlib
import tempfile

from pipeline import conversion

TEST_FOLDER = pathlib.Path(__file__).parent
PARAMETER_MODEL_METADATA = TEST_FOLDER / "data" / "parametermodel.json"
PARAMETER_MODEL_FILE = TEST_FOLDER / "data" / "parametermodel.csv"


def test_conversion():
    with open(PARAMETER_MODEL_METADATA, "r", encoding="utf-8") as metadata_file:
        metadata = json.load(metadata_file)

    with tempfile.TemporaryDirectory() as tmpdirname:
        oedatamodel_file = pathlib.Path(tmpdirname) / "oedatamodel.csv"
        assert not oedatamodel_file.exists()
        converter = conversion.OEDatamodelFromParametermodel(
            PARAMETER_MODEL_FILE, oedatamodel_file, metadata
        )
        converter.convert()
        assert oedatamodel_file.exists()


def test_energy_vectors():
    input_vector, output_vector = conversion.get_energy_vectors(
        "onshore wind farm", "efficiency value"
    )
    assert input_vector == "wind speed"
    assert output_vector == "electricity"
