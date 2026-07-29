import os
import json
import logging
import re
import shutil
import tempfile
import uuid
from collections.abc import Iterable
from datetime import timedelta
from pathlib import Path
from traceback import TracebackException
from typing import Any

import sentry_sdk
from constance import config
from django.conf import settings
from django.db import transaction
from django.forms.models import model_to_dict
from django.utils import timezone
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException
from qfieldcloud.authentication.models import AuthToken
from qfieldcloud.core.models import (
    ApplyJob,
    ApplyJobDelta,
    Delta,
    Job,
    PackageJob,
    ProcessProjectfileJob,
    Secret,
)
from qfieldcloud.core.utils2 import packages
from qfieldcloud.project.models import QgisProject
from qfieldcloud.project.utils.project_utils import get_qgis_major_version
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import urllib3

# TODO:
# Refactor worker orchestration into pluggable backends:
# - WorkerBackend interface
# - DockerBackend
# - KubernetesBackend
# So JobRun no longer depends directly on container runtime APIs.

k8s_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(
        (
            ApiException,
            urllib3.exceptions.HTTPError,
            ConnectionError,
        )
    ),
    reraise=True,
)

logger = logging.getLogger(__name__)

if settings.DEBUG:
    logger.setLevel(logging.DEBUG)

TIMEOUT_ERROR_EXIT_CODE = -1
TMP_FILE = Path(os.environ.get("QFC_SHARED_DIR", "/io"))

TRANSFORMATION_GRIDS_PATH = "/transformation_grids"

TOKEN_EXPIRATION_TIME_BUFFER_S = 60


class JobException(Exception):
    pass


class JobRun:
    container_timeout_secs = settings.QFIELDCLOUD_WORKER_TIMEOUT_S
    job_class = Job
    command = []

    debug_qgis_container_is_enabled = False
    """Whether the QGIS container is started with `debugpy` enabled, so that a debugger can attach to it."""

    qgis_images: dict[int, str] = {}
    """Mapping of QGIS major version to the corresponding QGIS Docker image name, e.g. `{"qgis3": "qfieldcloud-qgis3"}`."""

    def __init__(self, job_id: str) -> None:
        self.job = None
        self.container_timeout_secs = config.WORKER_TIMEOUT_S

        try:
            self.job_id = job_id
            self.job = self.job_class.objects.select_related().get(id=job_id)
            self.shared_tempdir = Path(tempfile.mkdtemp(dir=TMP_FILE))
        except Exception as err:
            feedback: dict[str, Any] = {}
            tb = TracebackException.from_exception(err)
            feedback["error"] = str(err)
            feedback["error_origin"] = "worker_wrapper"
            feedback["error_class"] = type(err).__name__
            feedback["error_stack"] = "".join(tb.format())

            logger.exception(
                "Uncaught exception when constructing JobRun",
                exc_info=err,
            )

            if self.job:
                self.job.status = Job.Status.FAILED
                self.job.feedback = feedback
                self.job.save(update_fields=["status", "feedback"])
                logger.exception(msg, exc_info=err)
            else:
                logger.critical(msg, exc_info=err)

        self.debug_qgis_container_is_enabled = bool(
            settings.DEBUG and settings.DEBUG_QGIS_DEBUGPY_PORT
        )

        self.qgis_images = {
            3: settings.QFIELDCLOUD_QGIS3_IMAGE_NAME,
            4: settings.QFIELDCLOUD_QGIS4_IMAGE_NAME,
        }

        if self.debug_qgis_container_is_enabled and self.job is not None:
            logger.warning(
                f"Debugging is enabled for job {self.job.id}. "
                "The worker will wait for debugger to attach on port "
                f"{settings.DEBUG_QGIS_DEBUGPY_PORT}."
            )

    def get_context(self) -> dict[str, Any]:
        context = model_to_dict(self.job)

        for key, value in model_to_dict(self.job.project).items():
            context[f"project__{key}"] = value

        context["project__id"] = self.job.project.id
        context["project__the_qgis_file_name"] = self.job.project.the_qgis_file_name

        return context

    def get_command(self) -> list[str]:
        context = self.get_context()

        if self.debug_qgis_container_is_enabled:
            debug_flags = [
                "-m",
                "debugpy",
                "--listen",
                f"0.0.0.0:{settings.DEBUG_QGIS_DEBUGPY_PORT}",
                "--wait-for-client",
            ]
        else:
            debug_flags = []
        return [
            p % context
            for p in ["python3", "entrypoint.py", *self.command]
        ]

    def get_environment(self) -> dict[str, str]:
        extra_envvars = {}

        pgservice_file_contents = ""

        for secret in Secret.objects.for_user_and_project(
            self.job.triggered_by,
            self.job.project,
        ):
            if secret.type == Secret.Type.ENVVAR:
                extra_envvars[secret.name] = secret.value

            elif secret.type == Secret.Type.PGSERVICE:
                pgservice_file_contents += f"\n{secret.value}"

            else:
                raise NotImplementedError(
                    f"Unknown secret type: {secret.type}"
                )

        token_expires_at = timezone.now() + timedelta(
            seconds=self.container_timeout_secs
            + TOKEN_EXPIRATION_TIME_BUFFER_S
        )

        token = AuthToken.objects.create(
            user=self.job.created_by,
            client_type=AuthToken.ClientType.WORKER,
            expires_at=token_expires_at,
        )

        return {
            **extra_envvars,
            "PGSERVICE_FILE_CONTENTS": pgservice_file_contents,
            "QFIELDCLOUD_EXTRA_ENVVARS": json.dumps(
                sorted(extra_envvars.keys())
            ),
            "QFIELDCLOUD_TOKEN": token.key,
            "QFIELDCLOUD_URL": settings.QFIELDCLOUD_WORKER_QFIELDCLOUD_URL,
            "JOB_ID": str(self.job.id),
            "PROJ_DOWNLOAD_DIR": TRANSFORMATION_GRIDS_PATH,
            "QT_QPA_PLATFORM": "offscreen",
        }

        return environment

    def get_qgis_image(self) -> str:
        if self.job.project.qgis_version:
            qgis_major_project_version = get_qgis_major_version(
                self.job.project.qgis_version
            )
        else:
            # The safe fallback is to use QGIS 3 until 4.2.x get's widely adopted
            qgis_major_project_version = 3

        if qgis_major_project_version not in self.qgis_images:
            raise JobException(
                f"Unsupported QGIS major version {qgis_major_project_version} for project {self.job.project.id} stored with {self.job.project.qgis_version}."
            )

        return self.qgis_images[qgis_major_project_version]

    def before_worker_run(self) -> None:
        pass

    def after_worker_run(self) -> None:
        pass

    def after_worker_exception(self) -> None:
        pass

    def _sanitize_name(self, name: str) -> str:
        """
        Kubernetes Job names must match DNS label rules.
        """

        name = name.lower()
        name = re.sub(r"[^a-z0-9-]", "-", name)
        name = re.sub(r"-+", "-", name)

        return name[:50].strip("-")

    def _build_job_manifest(
        self,
        command: list[str],
        environment: dict[str, str],
    ) -> dict[str, Any]:
        job_name = self._sanitize_name(
            f"qfc-job-{self.job.id}"
        )

        env_list = [
            {"name": key, "value": value}
            for key, value in environment.items()
        ]

        ## TODO this settings.QFC_K8S_JOB_BACKOFF_LIMIT needs to be added
        container_spec = {
            "name": "worker",
            "image": self.get_qgis_image(),
            "command": command,
            "env": env_list,
            "resources": {
                "requests": {
                    "memory": settings.QFC_K8S_MEMORY_REQUEST,
                    "cpu": settings.QFC_K8S_CPU_REQUEST,
                },
                "limits": {
                    "memory": settings.QFC_K8S_MEMORY_LIMIT,
                    "cpu": settings.QFC_K8S_CPU_LIMIT,
                },
            },
            "volumeMounts": [
                {
                    "name": "shared-data",
                    "mountPath": "/io",
                },
                {
                    "name": "transformation-grids",
                    "mountPath": TRANSFORMATION_GRIDS_PATH,
                    "readOnly": True,
                },
            ],
        }
        if self.debug_qgis_container_is_enabled:
            container_spec["ports"] = [
                {
                    "containerPort": settings.DEBUG_QGIS_DEBUGPY_PORT,
                }
            ]
        job_labels = {
            "app": "qfieldcloud-worker",
            "job_id": str(self.job.id),
            "project_id": str(self.job.project_id),
            "job_type": self.job.type,
        }
        manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "labels": job_labels,
            },
            "spec": {
                "ttlSecondsAfterFinished": 3600,
                "backoffLimit": settings.QFC_K8S_JOB_BACKOFF_LIMIT,
                "template": {
                    "metadata": {
                        "labels": job_labels,
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [container_spec],
                        "volumes": [
                            {
                                "name": "shared-data",
                                "persistentVolumeClaim": {
                                    "claimName": settings.QFC_SHARED_PVC_NAME,
                                },
                            },
                            {
                                "name": "transformation-grids",
                                "persistentVolumeClaim": {
                                    "claimName": settings.QFIELDCLOUD_TRANSFORMATION_GRIDS_VOLUME_NAME,
                                },
                            },
                        ],
                    },
                },
            },
        }
        
        return manifest
    
    @k8s_retry
    def _list_job_pods(
        self,
        core_api,
        namespace,
        job_name,
    ):
        return core_api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"job-name={job_name}",
        )

    @k8s_retry
    def _read_pod_logs(
        self,
        core_api,
        namespace,
        pod_name,
    ):
        return core_api.read_namespaced_pod_log(
            name=pod_name,
            namespace=namespace,
        )
    
    @k8s_retry
    def _create_job(self, batch_api, namespace, manifest):
        return batch_api.create_namespaced_job(
            namespace=namespace,
            body=manifest,
        )
    
    @k8s_retry
    def _read_job_status(self, batch_api, job_name, namespace):
        return batch_api.read_namespaced_job_status(
            name=job_name,
            namespace=namespace,
        )

    def _load_kubernetes(self) -> None:
        """
        Try in-cluster config first.
        Fall back to local kubeconfig for development.
        """

        try:
            k8s_config.load_incluster_config()

        except Exception:
            k8s_config.load_kube_config()

    def _wait_for_job_completion(
        self,
        batch_api: k8s_client.BatchV1Api,
        namespace: str,
        job_name: str,
    ) -> tuple[bool, dict[str, Any]]:
        import time

        timeout = self.container_timeout_secs
        elapsed = 0

        while elapsed < timeout:
            job = self._read_job_status(
                batch_api=batch_api,
                job_name=job_name,
                namespace=namespace,
            )

            status = job.status

            if status.succeeded:
                return True, status.to_dict()

            if status.failed:
                return False, status.to_dict()

            time.sleep(5)
            elapsed += 5

        return False, {
            "timeout": True,
        }

    def _get_pod_logs(
        self,
        core_api: k8s_client.CoreV1Api,
        namespace: str,
        job_name: str,
    ) -> str:
        pods = self._list_job_pods(
            core_api=core_api,
            namespace=namespace,
            job_name=job_name,
        )

        if not pods.items:
            return "[QFC/K8S/1001] No pod found for job."

        pod_name = pods.items[0].metadata.name

        return self._read_pod_logs(
            core_api=core_api,
            namespace=namespace,
            pod_name=pod_name,
        )

    def _run_kubernetes_job(
        self,
        command: list[str],
    ) -> tuple[int, bytes]:
        self._load_kubernetes()

        batch_api = k8s_client.BatchV1Api()
        core_api = k8s_client.CoreV1Api()

        namespace = settings.QFC_K8S_NAMESPACE

        environment = self.get_environment()

        manifest = self._build_job_manifest(
            command,
            environment,
        )

        job_name = manifest["metadata"]["name"]

        logger.info(
            f"Creating Kubernetes Job {job_name}"
        )

        #### Remove and change to worker started at
        self.job.docker_started_at = timezone.now()
        self.job.save(update_fields=["docker_started_at"])

        try:
            self._create_job(
                batch_api=batch_api,
                namespace=namespace,
                manifest=manifest,
            )

        except ApiException as err:
            logger.exception(
                "Failed to create Kubernetes Job",
                exc_info=err,
            )

            raise

        self.job.container_id = job_name
        self.job.save(update_fields=["container_id"])

        success, status = self._wait_for_job_completion(
            batch_api,
            namespace,
            job_name,
        )

        #### Remove and change to worker finished at
        self.job.docker_finished_at = timezone.now()
        self.job.save(update_fields=["docker_finished_at"])

        logs = self._get_pod_logs(
            core_api,
            namespace,
            job_name,
        )

        logger.info(
            f"Kubernetes Job {job_name} finished: {status}"
        )

        if not success:
            return TIMEOUT_ERROR_EXIT_CODE, logs.encode()

        return 0, logs.encode()

    def run(self):
        feedback = {}

        try:
            self.job.status = Job.Status.STARTED
            self.job.started_at = timezone.now()

            self.job.save(update_fields=["status", "started_at"])

            concurrent_jobs_count = (
                self.job.project.jobs.filter(
                    status__in=[
                        Job.Status.QUEUED,
                        Job.Status.STARTED,
                    ],
                )
                .exclude(pk=self.job.pk)
                .count()
            )

            if concurrent_jobs_count > 0:
                self.job.status = Job.Status.PENDING
                self.job.started_at = None

                self.job.save(
                    update_fields=["status", "started_at"]
                )

                logger.warning(
                    f"Concurrent jobs occurred for job {self.job}."
                )

                sentry_sdk.capture_message(
                    f"Concurrent jobs occurred for job {self.job}."
                )

                return

            self.before_worker_run()

            command = self.get_command()

            exit_code, output = self._run_kubernetes_job(
                command
            )

            try:
                self.job.refresh_from_db()
            except Job.DoesNotExist as err:
                logger.error(
                    "Failed to update job status, probably does not exist in the database.",
                    exc_info=err,
                )
                return

            if exit_code == TIMEOUT_ERROR_EXIT_CODE:
                feedback["error"] = "Worker timeout error."
                feedback["error_type"] = "TIMEOUT"
                feedback["error_class"] = ""
                feedback["error_origin"] = "container"
                feedback["error_stack"] = ""
            else:
                try:
                    feedback_path = self.shared_tempdir.joinpath("feedback.json")

                    if not feedback_path.exists():
                        fallback_feedback_path = TMP_FILE.joinpath("feedback.json")
                        if fallback_feedback_path.exists():
                            feedback_path = fallback_feedback_path

                    with open(feedback_path) as f:
                        feedback = json.load(f)

                    if feedback.get("error"):
                        feedback["error_origin"] = "container"

                except Exception as err:  # noqa: BLE001
                    if not isinstance(feedback, dict):
                        feedback = {"error_feedback": feedback}

                    tb = TracebackException.from_exception(err)
                    feedback["error"] = str(err)
                    feedback["error_origin"] = "worker_wrapper"
                    feedback["error_class"] = type(err).__name__
                    feedback["error_stack"] = "".join(tb.format())

            feedback["container_exit_code"] = exit_code

            self.job.output = output.decode("utf-8")
            self.job.feedback = feedback

            self.job.save(
                update_fields=["output", "feedback"]
            )

            if exit_code != 0:
                self.job.status = Job.Status.FAILED

                self.job.save(update_fields=["status"])

                self.after_worker_exception()

                return

            self.job.project.refresh_from_db()

            self.after_worker_run()

            shutil.rmtree(
                str(self.shared_tempdir),
                ignore_errors=True,
            )

            self.job.finished_at = timezone.now()
            self.job.status = Job.Status.FINISHED

            self.job.save(
                update_fields=["status", "finished_at"]
            )

            # Global error handler when handling a job
        except Exception as err:  # noqa: BLE001
            tb = TracebackException.from_exception(err)

            feedback["error"] = str(err)
            feedback["error_origin"] = "worker_wrapper"
            feedback["error_class"] = type(err).__name__
            feedback["error_stack"] = "".join(tb.format())

            logger.exception(
                "Failed Kubernetes job execution",
                exc_info=err,
            )

            try:
                self.job.status = Job.Status.FAILED
                self.job.feedback = feedback
                self.job.finished_at = timezone.now()

                self.after_worker_exception()

                self.job.save(
                    update_fields=[
                        "status",
                        "feedback",
                        "finished_at",
                    ]
                )

            except Exception:
                logger.exception(
                    "Failed updating failed job state"
                )

class PackageJobRun(JobRun):
    job_class = PackageJob

    command = [
        "package",
        "%(project__id)s",
        "%(project__the_qgis_file_name)s",
        "%(project__packaging_offliner)s",
    ]

    data_last_packaged_at = None

    ## TODO check if renaming can be done
    def before_worker_run(self) -> None:
        self.data_last_packaged_at = timezone.now()

    def after_worker_run(self) -> None:
        self.job.project.data_last_packaged_at = (
            self.data_last_packaged_at
        )

        self.job.project.save(
            update_fields=("data_last_packaged_at",)
        )

        packages.delete_obsolete_packages(
            projects=[self.job.project]
        )


class ApplyDeltaJobRun(JobRun):
    job_class = ApplyJob

    command = [
        "apply_deltas",
        "%(project__id)s",
        "%(project__the_qgis_file_name)s",
    ]

    def __init__(self, job_id: str) -> None:
        super().__init__(job_id)

        if self.job.overwrite_conflicts:
            self.command = [
                *self.command,
                "--overwrite-conflicts",
            ]
    
    def _prepare_deltas(self, deltas: Iterable[Delta]) -> dict[str, Any]:
        delta_contents = []
        delta_client_ids = []

        for delta in deltas:
            delta_contents.append(delta.content)

            if "clientId" in delta.content:
                delta_client_ids.append(delta.content["clientId"])

        local_to_remote_pk_deltas = Delta.objects.filter(
            client_id__in=delta_client_ids,
            last_modified_pk__isnull=False,
        ).values(
            "client_id", "content__localLayerId", "content__localPk", "last_modified_pk"
        )

        client_pks_map = {}

        for delta_with_modified_pk in local_to_remote_pk_deltas:
            key = f"{delta_with_modified_pk['client_id']}__{delta_with_modified_pk['content__localLayerId']}__{delta_with_modified_pk['content__localPk']}"
            client_pks_map[key] = delta_with_modified_pk["last_modified_pk"]

        deltafile_contents = {
            "deltas": delta_contents,
            "files": [],
            "id": str(uuid.uuid4()),
            "project": str(self.job.project.id),
            "version": "1.0",
            "clientPks": client_pks_map,
        }

        return deltafile_contents

    @transaction.atomic()
    def before_worker_run(self) -> None:
        deltas = self.job.deltas_to_apply.all()
        deltafile_contents = self._prepare_deltas(deltas)

        self.delta_ids = [d.id for d in deltas]

        ApplyJobDelta.objects.filter(
            apply_job_id=self.job_id,
            delta_id__in=self.delta_ids,
        ).update(status=Delta.Status.STARTED)

        self.job.deltas_to_apply.update(last_status=Delta.Status.STARTED)

        with open(self.shared_tempdir.joinpath("deltafile.json"), "w") as f:
            json.dump(deltafile_contents, f)

    def after_worker_run(self) -> None:
        delta_feedback = self.job.feedback["outputs"]["apply_deltas"]["delta_feedback"]
        is_data_modified = False

        for feedback in delta_feedback:
            delta_id = feedback["delta_id"]
            status = feedback["status"]
            modified_pk = feedback["modified_pk"]

            if status == "status_applied":
                status = Delta.Status.APPLIED
                is_data_modified = True
            elif status == "status_conflict":
                status = Delta.Status.CONFLICT
            elif status == "status_apply_failed":
                status = Delta.Status.NOT_APPLIED
            else:
                status = Delta.Status.ERROR
                # not certain what happened
                is_data_modified = True

            Delta.objects.filter(pk=delta_id).update(
                last_status=status,
                last_feedback=feedback,
                last_modified_pk=modified_pk,
                last_apply_attempt_at=self.job.started_at,
                last_apply_attempt_by=self.job.created_by,
            )

            ApplyJobDelta.objects.filter(
                apply_job_id=self.job_id,
                delta_id=delta_id,
            ).update(
                status=status,
                feedback=feedback,
                modified_pk=modified_pk,
            )

        if is_data_modified:
            self.job.project.data_last_updated_at = timezone.now()
            self.job.project.save(update_fields=("data_last_updated_at",))

    def after_worker_exception(self) -> None:
        Delta.objects.filter(
            id__in=self.delta_ids,
        ).update(
            last_status=Delta.Status.ERROR,
            last_feedback=None,
            last_modified_pk=None,
            last_apply_attempt_at=self.job.started_at,
            last_apply_attempt_by=self.job.created_by,
        )

        ApplyJobDelta.objects.filter(
            apply_job_id=self.job_id,
            delta_id__in=self.delta_ids,
        ).update(
            status=Delta.Status.ERROR,
            feedback=None,
            modified_pk=None,
        )

class ProcessProjectfileJobRun(JobRun):
    job_class = ProcessProjectfileJob

    command = [
        "process_projectfile",
        "%(project__id)s",
        "%(project__the_qgis_file_name)s",
    ]

    def get_context(self, *args) -> dict[str, Any]:
        context = super().get_context(*args)

        assert context.get("project__the_qgis_file_name")

        return context

    def after_worker_run(self) -> None:
        project = self.job.project

        project.project_details = self.job.feedback[
            "outputs"
        ]["project_details"]["project_details"]

        # Since the `Project.qgis_version` field is newly added, we want to backfill it for old projects that didn't have it set,
        # but the `process_projectfile` job can detect the QGIS version from the project file and return it in the feedback, we can set it here.
        # NOTE `Project.qgis_version` is used by `wrapper.JobRun.get_qgis_image()` to detect
        # the correct QGIS docker image to run the job.
        # Therefore, we cannot use the very similar `QgisProject.qgis_version` field,
        # which is not populated until the first `process_projectfile` job is run.
        if self.job.project.qgis_version is None and project.project_details.get(
            "qgis_version"
        ):
            project.qgis_version = project.project_details["qgis_version"]
            update_fields.append("qgis_version")

        project.save(update_fields=update_fields)

    def after_docker_exception(self) -> None:
        project = self.job.project

        if hasattr(project, "qgis_project"):
            project.qgis_project.delete()

        # TODO @manylon: keep in sync with `QgisProject` until `Project.project_details` is dropped, see https://app.clickup.com/t/2192114/QF-8600
        if project.project_details is not None:
            project.project_details = None
            project.save(update_fields=("project_details",))

class CreateProjectJobRun(JobRun):
    job_class = Job

    command = [
        "create_project",
        "%(project__id)s",
    ]

def cancel_orphaned_workers() -> None:
    logger.info("cancel_orphaned_workers disabled for Kubernetes backend")