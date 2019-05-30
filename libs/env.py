import os
import re


class Env(object):
    def __init__(self):
        pass

    @property
    def project_path(self):
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    @property
    def config_path(self):
        return os.path.join(self.project_path, 'config')

    @property
    def libs_path(self):
        return os.path.join(self.project_path, 'libs')

    @property
    def utils_path(self):
        return os.path.join(self.project_path, 'utils')

    @property
    def log_path(self):
        return os.path.join(self.project_path, 'log')

    @property
    def screenshot_path(self):
        return os.path.join(self.project_path, 'screenshot')

    @property
    def cache_path(self):
        return os.path.join(self.project_path, 'cache')

    @property
    def resource_path(self):
        return os.path.join(self.project_path, 'resources')

    @property
    def bin_path(self):
        return os.path.join(self.project_path, 'bin')

    @property
    def temp_path(self):
        path = os.path.join(self.project_path, 'temp')
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    @property
    def apks_path(self):
        path = os.path.join(self.project_path, 'apks')
        if not os.path.exists(path):
            os.mkdir(path)
        return path

    @property
    def sep(self):
        return os.path.sep



if __name__ == '__main__':
    env = Env()
