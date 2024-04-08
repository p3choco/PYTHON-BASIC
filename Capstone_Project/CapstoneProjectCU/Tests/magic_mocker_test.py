import json
import sys
import pytest
from unittest.mock import patch
from configurator import Configurator
from file_outputter import FileOutputter
from magic_mocker import MagicMocker

@pytest.fixture(autouse=True)
def mock_sys_argv():
    testargs = ['']
    with patch.object(sys, 'argv', testargs):
        yield

@pytest.mark.parametrize("data_type, expected_type", [
    ("timestamp", float),
    ("str", str),
    ("int", int)
])
def test_fill_with_mock_data_data_types(data_type, expected_type):
    mocker = MagicMocker()
    schema_dict = {"test_key": f"{data_type}:rand"}
    result = mocker.fill_with_mock_data(schema_dict)
    assert isinstance(result.get("test_key", None), expected_type)

def test_fill_with_mock_data_wrong_data_types():
    mocker = MagicMocker()
    schema_dict = {"test_key": "None:rand"}
    with pytest.raises(SystemExit) as e:
        result = mocker.fill_with_mock_data(schema_dict)

    assert e.type == SystemExit


@pytest.mark.parametrize("schema", [
    ({"date": "timestamp:", "name": "str:rand", "age": "int:rand(1, 90)"}),
    ({"date": "timestamp:e", "name": "str:rand", "age": "int:rand(1, 90)"}),
    ({"name": "str:example", "age": "int:25"})
])
def test_fill_with_mock_data_schemas(schema):
    mocker = MagicMocker()
    result = mocker.fill_with_mock_data(schema)
    assert all(key in result for key in schema)

@pytest.fixture
def create_temp_schema_file(tmp_path):
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps({"test": "str:example"}))
    return str(schema_file)

def test_get_schema_dict_from_file(create_temp_schema_file):
    mocker = MagicMocker()
    with patch.object(mocker, 'data_schema', create_temp_schema_file):
        result = mocker.get_mocked_data_dict()
    assert "test" in result and result["test"] == "example"


def test_clear_path_action(tmp_path):
    file_path = tmp_path / "test_file.jsonl"
    file_path.touch()
    assert file_path.exists()

    file_outputter = FileOutputter()
    file_outputter.handle_path(str(tmp_path), True, "test_file")
    assert not file_path.exists()

def test_write_to_file(tmp_path):
    file_outputter = FileOutputter()
    data = [{"name": "test"}]
    file_path = tmp_path / "output.jsonl"
    file_outputter.write_to_file(data, str(file_path))

    with open(file_path) as f:
        content = json.load(f)
    assert content == data[0]

@pytest.mark.integration
def test_multiprocessing_file_creation(tmp_path):
    path_to_save_files = tmp_path
    files_count = 2
    file_name = 'test_output'
    file_prefix = 'random'
    data_schema = '{"name": "str:rand"}'
    data_lines = 1

    mocker = MagicMocker()
    mocker.path_to_save_files = str(path_to_save_files)
    mocker.files_count = files_count
    mocker.file_name = file_name
    mocker.file_prefix = file_prefix
    mocker.data_schema = data_schema
    mocker.data_lines = data_lines
    mocker.multiprocessing = 2

    mocker.run()

    generated_files = list(path_to_save_files.glob('*'))
    assert len(generated_files) == files_count

def test_file_name_getting_from_console():
    with patch('sys.argv', ['magic_mocker', '--file_name=test_file']):
        configurator = Configurator()
        assert configurator.args.file_name == "test_file"

