import os
from pathlib import Path

from py4j.java_gateway import JavaGateway


class CronHelper:

    def __init__(self):
        cron_utils = Path(os.path.abspath(__file__)).parent.absolute() / "cron-utils-9.2.0.jar"
        jg = JavaGateway.launch_gateway(classpath=str(cron_utils))
        jvm = jg.jvm
        j_cron_parser = jvm.com.cronutils.parser.CronParser
        j_cron_definition_builder = jvm.com.cronutils.model.definition.CronDefinitionBuilder
        j_cron_type = jvm.com.cronutils.model.CronType
        self._j_cron_mapper = jvm.com.cronutils.mapper.CronMapper
        self._unix_parser = j_cron_parser(j_cron_definition_builder.instanceDefinitionFor(j_cron_type.UNIX))
        self._quartz_parser = j_cron_parser(j_cron_definition_builder.instanceDefinitionFor(j_cron_type.QUARTZ))

    def unix_to_quartz(self, unix_cron: str) -> str:
        return self._j_cron_mapper.fromUnixToQuartz().map(self._unix_parser.parse(unix_cron)).asString()

    def quartz_to_unix(self, unix_cron: str) -> str:
        return self._j_cron_mapper.fromQuartzToUnix().map(self._quartz_parser.parse(unix_cron)).asString()


# def execution_date(unix_cron_statement, quartz_cron_statement, timestamp ) -> "dttm":
#     pass