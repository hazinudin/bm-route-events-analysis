"""
Standalone configuration loader (replaces SMD_Package.load_config.SMDConfigs).

Reads ``smd_config.json`` from the project root (two levels above this file).
"""

import json
import os


def _project_root():
    """Return the absolute path to the project root folder."""
    this_dir = os.path.dirname(os.path.abspath(__file__))
    # aadt_recalc/ is one level below project root
    return os.path.dirname(this_dir)


class SMDConfigs(object):
    """
    Loads ``smd_config.json`` from the project root and exposes every
    top-level key as an instance attribute.
    """

    def __init__(self, config_file='smd_config.json'):
        file_path = os.path.join(_project_root(), config_file)
        with open(file_path) as fh:
            config_dict = json.load(fh)
        for key, value in config_dict.items():
            setattr(self, key, value)

    @staticmethod
    def smd_dir():
        """Return the project root path (same concept as the original)."""
        return _project_root()
