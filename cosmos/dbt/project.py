from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from cosmos.constants import (
    DBT_DEPENDENCIES_FILE_NAMES,
    DBT_LOG_DIR_NAME,
    DBT_PARTIAL_PARSE_FILE_NAME,
    DBT_TARGET_DIR_NAME,
    PACKAGE_LOCKFILE_YML,
)
from cosmos.log import get_logger

logger = get_logger(__name__)


def has_non_empty_dependencies_file(project_path: Path) -> bool:
    """
    Check if the dbt project has dependencies.yml or packages.yml.

    :param project_path: Path to the project
    :returns: True or False
    """
    project_dir = Path(project_path)
    has_deps = False
    for filename in DBT_DEPENDENCIES_FILE_NAMES:
        filepath = project_dir / filename
        if filepath.exists() and filepath.stat().st_size > 0:
            has_deps = True
            break

    if not has_deps:
        logger.info(f"Project {project_path} does not have {DBT_DEPENDENCIES_FILE_NAMES}")
    return has_deps


def create_symlinks(project_path: Path, project_conn_id: str, tmp_dir: Path, ignore_dbt_packages: bool) -> None:
    """Helper function to create symlinks to the dbt project files."""
    ignore_paths = [DBT_LOG_DIR_NAME, DBT_TARGET_DIR_NAME, PACKAGE_LOCKFILE_YML, "profiles.yml"]
    if ignore_dbt_packages:
        # this is linked to dbt deps so if dbt deps is true then ignore existing dbt_packages folder
        ignore_paths.append("dbt_packages")
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    if project_conn_id:
        # Handle S3 path copying
        s3_hook = S3Hook()
        bucket_name, key_prefix = project_path.parts[0], '/'.join(project_path.parts[1:])

        for obj in s3_hook.list_keys(bucket_name=bucket_name, prefix=key_prefix):
            relative_path = Path(obj).relative_to(key_prefix)
            local_path = tmp_dir / relative_path
            logger.info(f"Downloading {obj} to {local_path}")
            dir = local_path.parent
            dir.mkdir(parents=True, exist_ok=True)
            for child_name in os.listdir(dir):
                logger.info(f"child_name: {child_name}")
            # Download the file to the local path
            s3_hook.download_file(bucket_name=bucket_name, key=obj, local_path=str(local_path))
    else:
        # Handle local symlinking
        for child_name in os.listdir(project_path):
            if child_name.name not in ignore_paths:
                os.symlink(project_path / child_name, tmp_dir / child_name.name)



def get_partial_parse_path(project_dir_path: Path) -> Path:
    """
    Return the partial parse (partial_parse.msgpack) path for a given dbt project directory.
    """
    return project_dir_path / DBT_TARGET_DIR_NAME / DBT_PARTIAL_PARSE_FILE_NAME


@contextmanager
def environ(env_vars: dict[str, str]) -> Generator[None, None, None]:
    """Temporarily set environment variables inside the context manager and restore
    when exiting.
    """
    original_env = {key: os.getenv(key) for key in env_vars}
    os.environ.update(env_vars)
    try:
        yield
    finally:
        for key, value in original_env.items():
            if value is None:
                del os.environ[key]
            else:
                os.environ[key] = value


@contextmanager
def change_working_directory(path: str) -> Generator[None, None, None]:
    """Temporarily changes the working directory to the given path, and then restores
    back to the previous value on exit.
    """
    previous_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous_cwd)
