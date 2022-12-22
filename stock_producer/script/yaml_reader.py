import yaml


def read_yaml_file(file_dir):
    with open(file_dir, "r") as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return data
