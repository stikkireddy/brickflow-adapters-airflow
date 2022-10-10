import datetime
import functools
import logging
import os
import subprocess
import sys
import time
import types
from typing import List, Optional, Dict, Union, Callable

from airflow import macros
from airflow import DAG
from airflow.models import XCOM_RETURN_KEY, BaseOperator, Pool
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.utils.weight_rule import WeightRule

from brickflow.context import (
    ctx,
    BrickflowTaskComs,
    BRANCH_SKIP_EXCEPT,
    SKIP_EXCEPT_HACK,
)
from brickflow.engine.compute import Compute
from brickflow.engine.task import (
    TaskSettings,
    BrickflowTriggerRule,
    UnsupportedBrickflowTriggerRuleError,
    TaskType,
    Task,
    CustomTaskResponse,
)
from brickflow.engine.workflow import Workflow, WorkflowPermissions

from brickflow_airflow_110 import resolve_py4j_logging
from brickflow_airflow_110.cronhelper import CronHelper

LOGGER = logging.getLogger(__name__)


def _bash_empty_on_kill(self):  # pylint:disable=unused-argument
    pass


def _skip_all_except(
        self, ti: "FakeTaskInstance", branch_task_ids
):  # pylint:disable=unused-argument
    ti.xcom_push(BRANCH_SKIP_EXCEPT, branch_task_ids)


def _short_circuit_execute(self, context):
    condition = super(ShortCircuitOperator, self).execute(context)
    self.log.info("Condition result is %s", condition)

    if condition:
        self.log.info("Proceeding with downstream tasks...")
        return

    self.log.info("Skipping downstream tasks...")
    ti = context["ti"]
    ti.xcom_push(BRANCH_SKIP_EXCEPT, SKIP_EXCEPT_HACK)


def _bash_execute(self, context):  # pylint:disable=unused-argument
    p = None
    returncode = None
    start = time.time()
    env = self.env
    if env is None:
        env = os.environ.copy()

    LOGGER.info("Command: %s", self.bash_command)
    from airflow.utils.file import TemporaryDirectory

    with TemporaryDirectory(prefix="airflowtmp") as tmp_dir:
        try:
            p = subprocess.Popen(  # pylint:disable=consider-using-with
                self.bash_command,
                shell=True,
                cwd=tmp_dir,
                executable="/bin/bash",
                stderr=subprocess.STDOUT,
                stdout=subprocess.PIPE,
                universal_newlines=True,
                env=env,
            )
            for line in iter(p.stdout.readline, ""):
                resp = line
                LOGGER.info("[STDOUT]: %s", line.rstrip())
            returncode = p.wait()
            p = None
            sys.stdout.flush()
            if returncode != 0:
                raise subprocess.CalledProcessError(returncode, self.bash_command)
        finally:
            end = time.time()
            if p is not None:
                p.terminate()
                p.wait()
            LOGGER.info("Command: exited with return code %s", returncode)
            LOGGER.info("Command took %s seconds", end - start)

        if self.xcom_push_flag is True:
            return resp[:-1]  # skip newline char at end
        return


class CrossDagXComsNotSupportedError(Exception):
    pass


class XComsPullMultipleTaskIdsError(Exception):
    pass


class FakeTaskInstance(object):
    def __init__(
            self,
            task_id,
            task,
            execution_date,
    ):
        self._execution_date = execution_date
        self._task = task
        self._task_id = task_id

    def xcom_push(self, key, value):
        ctx.task_coms.put(task_id=self._task_id, key=key, value=value)

    def xcom_pull(self, task_ids, key=XCOM_RETURN_KEY, dag_id=None):
        if dag_id is not None:
            raise CrossDagXComsNotSupportedError(
                "Cross dag xcoms not supported in framework raise feature request."
            )
        if isinstance(task_ids, list) and len(task_ids) > 1:
            raise XComsPullMultipleTaskIdsError(
                "Currently xcoms pull only supports one task_id please raise feature "
                "request."
            )
        task_id = task_ids[0] if isinstance(task_ids, list) else task_ids
        return ctx.task_coms.get(task_id, key)

    @property
    def execution_date(self):
        return self._execution_date

    @property
    def task(self):
        return self._task


def with_task_logger(f):
    @functools.wraps(f)
    def func(*args, **kwargs):
        task_id = args[1] if not kwargs else kwargs["task_id"]
        logger = logging.getLogger()  # Logger
        back_up_logging_handlers = logger.handlers
        logger.handlers = []
        logger_handler = logging.StreamHandler()  # Handler for the logger
        logger.addHandler(logger_handler)

        # First, generic formatter:
        resolve_py4j_logging()
        logger_handler.setFormatter(
            logging.Formatter(
                f"[%(asctime)s] [%(levelname)s] [airflow_1_10_task:{task_id}] "
                "{%(module)s.py:%(lineno)d} - %(message)s"
            )
        )
        resp = f(*args, **kwargs)

        logger.handlers = []
        for handler in back_up_logging_handlers:
            logger.addHandler(handler)

        return resp

    return func


class AirflowTaskDoesNotExistError(Exception):
    pass


class UnsupportedAirflowTaskFieldError(Exception):
    pass


class UnsupportedAirflowOperatorError(Exception):
    pass


class AirflowDagAdapter:
    UNSUPPORTED_TASK_NONE_FIELDS = {
        "email_on_retry": True,
        "email_on_failure": True,
        "sla": None,
        "execution_timeout": None,
        "on_failure_callback": None,
        "on_success_callback": None,
        "on_retry_callback": None,
        "inlets": [],
        "outlets": [],
        "task_concurrency": None,
        "run_as_user": None,
        "depends_on_past": False,
        "wait_for_downstream": False,
        "max_retry_delay": None,
        "priority_weight": 1,
        "weight_rule": WeightRule.DOWNSTREAM,
        "pool": Pool.DEFAULT_POOL_NAME,
        "pool_slots": 1,
        "resources": None,
        "executor_config": {},
        "email": None,
    }

    SUPPORTED_OPERATORS = [
        BranchPythonOperator,
        PythonOperator,
        BashOperator,
        ShortCircuitOperator,
    ]

    def __init__(self, dag, ts=None):

        self._dag: DAG = dag
        self._ts = ts or datetime.datetime.now()
        self._task_dict = self._get_task_dict()
        # must be run at the end to populate all fields and then generate ctx
        self._fake_xcoms = ctx.task_coms

    @property
    def dag(self):
        return self._dag

    def exists(self, task_id):
        if task_id in self._task_dict:
            return True
        raise AirflowTaskDoesNotExistError(
            f"Task with task_id: {task_id} does not exist!\n"
            f"Please take a look at the following tasks: "
            f"{list(self._task_dict.keys())}"
        )

    def is_branch_operator(self, task_id):
        if task_id in self._task_dict and isinstance(
                self._task_dict[task_id], BranchPythonOperator
        ):
            return True
        return False

    def get_task(self, task_id) -> BaseOperator:
        self.exists(task_id)
        return self._task_dict[task_id]

    @property
    def fake_xcoms(self) -> BrickflowTaskComs:
        return self._fake_xcoms

    def _get_task_dict(self):
        resp = {}
        for task in self._dag.tasks:
            if isinstance(task, BashOperator):
                f = types.MethodType(_bash_execute, task)
                task.execute = f
                task.on_kill = _bash_empty_on_kill
            elif isinstance(task, BranchPythonOperator):
                f = types.MethodType(_skip_all_except, task)
                task.skip_all_except = f
            elif isinstance(task, ShortCircuitOperator):
                f = types.MethodType(_short_circuit_execute, task)
                task.execute = f
            resp[task.task_id] = task
        return resp

    # def get_quartz_syntax(self):
    #     spark = SparkSession.getActiveSession()
    #     jvm = spark._jvm
    #     J_CronParser = jvm.com.cronutils.parser.CronParser
    #     J_CronDefinitionBuilder = jvm.com.cronutils.model.definition.CronDefinitionBuilder
    #     J_CronType = jvm.com.cronutils.model.CronType
    #     J_CronMapper = jvm.com.cronutils.mapper.CronMapper
    #     parser = J_CronParser(J_CronDefinitionBuilder.instanceDefinitionFor(J_CronType.UNIX))
    #     return J_CronMapper.fromUnixToQuartz().map(parser.parse(self._dag.schedule_interval)).asString()

    def execution_timestamp(self):
        previous, following, normalized = (
            self._dag.previous_schedule(self._ts),
            self._dag.following_schedule(self._ts),
            self._dag.normalize_schedule(self._ts),
        )
        # ts provided is on the dot on one of the schedules
        if previous != following != normalized:
            return normalized
        # ts is a little bit after the scheduled date
        return previous

    def _execution_time(self):
        return self.execution_timestamp().strftime("%H:%M:%S")

    def validate_task(self, task_id):
        self.validate_task_fields(task_id)
        task: BaseOperator = self._task_dict[task_id]
        if type(task) in self.SUPPORTED_OPERATORS:
            return True
        raise UnsupportedAirflowOperatorError(
            f"Unsupported airflow operator: {type(task)} for task: {task_id}"
        )

    def validate_task_fields(self, task_id):
        self.exists(task_id)
        task: BaseOperator = self._task_dict[task_id]
        unsupported_fields = []
        for field, default_value in self.UNSUPPORTED_TASK_NONE_FIELDS.items():
            value = getattr(task, field)
            if value != default_value:
                unsupported_fields.append(field)
        if unsupported_fields:
            raise UnsupportedAirflowTaskFieldError(
                f"Unsupported fields: {unsupported_fields} for task: {task_id}"
            )

    @with_task_logger
    def execute(self, task_id):
        task = self._task_dict[task_id]
        # render templates with available context variables
        from jinja2 import Environment, BaseLoader

        task_context = self._create_task_context(task_id, task)
        env = Environment(loader=BaseLoader)
        env.globals.update({"macros": macros, "ti": task_context})

        task.render_template_fields(task_context, jinja_env=env)
        return task.execute(context=task_context)
        # if task.do_xcom_push is True:
        #     task_context["ti"].xcom_push("return_value", resp)
        # return resp

    def _create_task_context(self, task_id, task):
        execution_ts = self.execution_timestamp()
        return {
            "execution_date": execution_ts,
            "ds": execution_ts.strftime("%Y-%m-%d"),
            "ds_nodash": execution_ts.strftime("%Y%m%d"),
            "ts": str(execution_ts),
            "ts_nodash": execution_ts.strftime("%Y%m%d%H%M%S"),
            "ti": FakeTaskInstance(task_id, task, execution_ts),
        }


class BrickflowWorkflowWithAirflow110(Workflow):
    def __init__(
            self,
            name,
            default_compute: Compute = Compute(compute_id="default"),
            compute: List[Compute] = None,
            existing_cluster=None,
            default_task_settings: Optional[TaskSettings] = None,
            tags: Dict[str, str] = None,
            max_concurrent_runs: int = 1,
            permissions: WorkflowPermissions = None,
            airflow_110_dag: "DAG" = None,
    ):
        super().__init__(
            name,
            default_compute,
            compute,
            existing_cluster,
            default_task_settings,
            tags,
            max_concurrent_runs,
            permissions,
        )

        # todo: add defaults
        self._airflow_110_dag = self._get_airflow_dag(airflow_110_dag)

    @staticmethod
    def _get_airflow_dag(dag110):
        if dag110 is None:
            return None
        try:
            return AirflowDagAdapter(dag110)
        except ImportError:
            # TODO: log error
            return None

    @property
    def airflow_dag(self):
        return self._airflow_110_dag

    def _custom_airflow110_task_execute(self, task: Task) -> CustomTaskResponse:
        resolve_py4j_logging()
        task_key = task.name
        resp = self.airflow_dag.execute(task_id=task_key)
        return CustomTaskResponse(
            resp, push_return_value=self.airflow_dag.get_task(task_key).do_xcom_push
        )

    def bind_airflow_task(
            self,
            name: str,
            compute: Optional[Compute] = None,
            depends_on: Optional[List[Union[Callable, str]]] = None,
    ):
        self._airflow_110_dag.exists(name)
        trigger_rule = self._airflow_110_dag.get_task(name).trigger_rule
        if BrickflowTriggerRule.is_valid(trigger_rule) is False:
            raise UnsupportedBrickflowTriggerRuleError(
                f"Unsupported trigger rule: {trigger_rule} for task: {name}"
            )
        self._airflow_110_dag.validate_task(name)
        return self.task(
            name=name,
            compute=compute,
            task_type=TaskType.CUSTOM_PYTHON_TASK,
            depends_on=depends_on,
            trigger_rule=BrickflowTriggerRule[trigger_rule.upper()],
            custom_execute_callback=self._custom_airflow110_task_execute,
        )


if __name__ == "__main__":
    cron = CronHelper().quartz_to_unix("0 0 10 * * ?")
    print(cron)
    # "0 0,8,15 * * *"
    da = AirflowDagAdapter(DAG("test", schedule_interval=cron))
    print(datetime.datetime.utcnow())
    print(da.execution_timestamp())