from configparser import ConfigParser
import os


def get_config():
    config = ConfigParser()
    dirname = os.path.dirname(__file__)
    config_filename = os.path.join(dirname, "config/my_config.cfg")
    config.read(config_filename)
    return config
