"""Test disable_node_owner functionality"""

import pytest
from pathlib import Path

from cosmos.config import RenderConfig
from cosmos.airflow.graph import create_test_task_metadata, create_task_metadata
from cosmos.constants import ExecutionMode, TestIndirectSelection, DbtResourceType
from cosmos.dbt.graph import DbtNode


class TestDisableNodeOwner:
    """Test cases for the disable_node_owner parameter in RenderConfig"""

    def test_render_config_disable_node_owner_default(self):
        """Test that disable_node_owner defaults to False for backwards compatibility"""
        render_config = RenderConfig()
        assert render_config.disable_node_owner is False

    def test_render_config_disable_node_owner_explicit(self):
        """Test that disable_node_owner can be set explicitly"""
        render_config = RenderConfig(disable_node_owner=True)
        assert render_config.disable_node_owner is True

    @pytest.fixture
    def test_node_with_owner(self):
        """Create a test node with an owner"""
        return DbtNode(
            unique_id="model.test_project.test_model",
            resource_type=DbtResourceType.MODEL,
            depends_on=[],
            file_path=Path("models/test_model.sql"),
            tags=[],
            config={"meta": {"owner": "test_owner"}}
        )

    @pytest.fixture
    def test_node_without_owner(self):
        """Create a test node without an owner"""
        return DbtNode(
            unique_id="model.test_project.test_model_no_owner",
            resource_type=DbtResourceType.MODEL,
            depends_on=[],
            file_path=Path("models/test_model_no_owner.sql"),
            tags=[],
            config={}
        )

    def test_create_task_metadata_with_owner_enabled(self, test_node_with_owner):
        """Test create_task_metadata preserves owner when disable_node_owner=False"""
        render_config = RenderConfig(disable_node_owner=False)
        
        task_metadata = create_task_metadata(
            node=test_node_with_owner,
            execution_mode=ExecutionMode.LOCAL,
            args={},
            dbt_dag_task_group_identifier="test_dag",
            render_config=render_config
        )
        
        assert task_metadata.owner == "test_owner"

    def test_create_task_metadata_with_owner_disabled(self, test_node_with_owner):
        """Test create_task_metadata ignores owner when disable_node_owner=True"""
        render_config = RenderConfig(disable_node_owner=True)
        
        task_metadata = create_task_metadata(
            node=test_node_with_owner,
            execution_mode=ExecutionMode.LOCAL,
            args={},
            dbt_dag_task_group_identifier="test_dag",
            render_config=render_config
        )
        
        assert task_metadata.owner == ""

    def test_create_task_metadata_without_owner_enabled(self, test_node_without_owner):
        """Test create_task_metadata with no owner when disable_node_owner=False"""
        render_config = RenderConfig(disable_node_owner=False)
        
        task_metadata = create_task_metadata(
            node=test_node_without_owner,
            execution_mode=ExecutionMode.LOCAL,
            args={},
            dbt_dag_task_group_identifier="test_dag",
            render_config=render_config
        )
        
        assert task_metadata.owner == ""

    def test_create_task_metadata_without_owner_disabled(self, test_node_without_owner):
        """Test create_task_metadata with no owner when disable_node_owner=True"""
        render_config = RenderConfig(disable_node_owner=True)
        
        task_metadata = create_task_metadata(
            node=test_node_without_owner,
            execution_mode=ExecutionMode.LOCAL,
            args={},
            dbt_dag_task_group_identifier="test_dag",
            render_config=render_config
        )
        
        assert task_metadata.owner == ""

    def test_create_task_metadata_no_render_config(self, test_node_with_owner):
        """Test create_task_metadata preserves owner when render_config is None"""
        task_metadata = create_task_metadata(
            node=test_node_with_owner,
            execution_mode=ExecutionMode.LOCAL,
            args={},
            dbt_dag_task_group_identifier="test_dag",
            render_config=None
        )
        
        assert task_metadata.owner == "test_owner"

    def test_create_test_task_metadata_with_owner_enabled(self, test_node_with_owner):
        """Test create_test_task_metadata preserves owner when disable_node_owner=False"""
        render_config = RenderConfig(disable_node_owner=False)
        
        test_task_metadata = create_test_task_metadata(
            test_task_name="test_task",
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args={},
            node=test_node_with_owner,
            render_config=render_config
        )
        
        assert test_task_metadata.owner == "test_owner"

    def test_create_test_task_metadata_with_owner_disabled(self, test_node_with_owner):
        """Test create_test_task_metadata ignores owner when disable_node_owner=True"""
        render_config = RenderConfig(disable_node_owner=True)
        
        test_task_metadata = create_test_task_metadata(
            test_task_name="test_task",
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args={},
            node=test_node_with_owner,
            render_config=render_config
        )
        
        assert test_task_metadata.owner == ""

    def test_create_test_task_metadata_no_node(self):
        """Test create_test_task_metadata with no node (TestBehavior.AFTER_ALL case)"""
        render_config = RenderConfig(disable_node_owner=True)
        
        test_task_metadata = create_test_task_metadata(
            test_task_name="test_task",
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args={},
            node=None,
            render_config=render_config
        )
        
        assert test_task_metadata.owner == ""

    def test_create_test_task_metadata_no_render_config(self, test_node_with_owner):
        """Test create_test_task_metadata preserves owner when render_config is None"""
        test_task_metadata = create_test_task_metadata(
            test_task_name="test_task",
            execution_mode=ExecutionMode.LOCAL,
            test_indirect_selection=TestIndirectSelection.EAGER,
            task_args={},
            node=test_node_with_owner,
            render_config=None
        )
        
        assert test_task_metadata.owner == "test_owner" 