import os
import re


class Env(object):
    def __init__(self):
        self._query_jar = 'athena_query_kika_tool-1.0.jar'
        self._connector_jar = 'mysql-connector-java-5.1.40-bin.jar'
        self._jdbc_jar = 'AthenaJDBC41-1.0.0.jar'
        self._sestool_jar = 'sestool-1.0.jar'
        self._java_exec = '/usr/local/jdk1.8.0_121/bin/java'


    @property
    def athena_executor(self):
        return '{java} -classpath {query}:{jdbc}:{connector} com.kika.tech.athena_query_kika_tool.AthenaQueryTool AKIAIUO2VW53QUXXMCFQ '.format(
            java=self._java_exec,
            query=os.path.join(env.libs_path, self._query_jar),
            jdbc=os.path.join(env.libs_path, self._jdbc_jar),
            connector=os.path.join(env.libs_path, self._connector_jar)
        )

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
