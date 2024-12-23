import asyncio
import pytest
from unittest.mock import AsyncMock
from app.actions.handlers import (
    action_get_file_status,
    action_set_file_status,
    action_reprocess_file,
    PENDING_FILES,
    IN_PROGRESS_FILES,
    PROCESSED_FILES
)
from app.actions.configurations import FileStatus, GetFileStatusConfig, SetFileStatusConfig, ReprocessFileConfig

@pytest.mark.asyncio
async def test_action_get_file_status(mocker, integration_v2, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    action_config = GetFileStatusConfig(filename="test_file.xml")

    result = await action_get_file_status(integration_v2, action_config)

    mock_state_manager.group_ismember.assert_any_call(PENDING_FILES, "test_file.xml")
    assert result == {"file_status": FileStatus.PENDING.value}


@pytest.mark.asyncio
async def test_action_get_file_status_not_found(mocker, integration_v2, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)

    future = asyncio.Future()
    future.set_result(False)
    mock_state_manager.group_ismember.return_value = future

    action_config = GetFileStatusConfig(filename="non_existent_file.xml")

    result = await action_get_file_status(integration_v2, action_config)

    mock_state_manager.group_ismember.assert_any_call(PENDING_FILES, "non_existent_file.xml")
    mock_state_manager.group_ismember.assert_any_call(IN_PROGRESS_FILES, "non_existent_file.xml")
    mock_state_manager.group_ismember.assert_any_call(PROCESSED_FILES, "non_existent_file.xml")
    assert result == {"file_status": "Not found"}


@pytest.mark.asyncio
async def test_action_set_file_status(mocker, integration_v2, mock_state_manager, mock_file_storage):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    action_config = SetFileStatusConfig(filename="test_file.xml", status=FileStatus.IN_PROGRESS)

    result = await action_set_file_status(integration_v2, action_config)

    mock_state_manager.group_move.assert_any_call(
        from_group="ats_pending_files",
        to_group="ats_in_progress_files",
        values=[action_config.filename]
    )
    assert result == {"file_status": action_config.status.value, 'message': f"File status for '{action_config.filename}' in integration '{str(integration_v2.id)}' set to '{action_config.status.value}'."}


@pytest.mark.asyncio
async def test_action_set_file_status_not_found_set_file_to_pending(mocker, integration_v2, mock_state_manager, mock_file_storage):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)

    future = asyncio.Future()
    future.set_result(False)
    mock_state_manager.group_ismember.return_value = future

    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    action_config = SetFileStatusConfig(filename="non_existent_file.xml", status=FileStatus.IN_PROGRESS)

    result = await action_set_file_status(integration_v2, action_config)

    mock_state_manager.group_ismember.assert_any_call(PENDING_FILES, "non_existent_file.xml")
    mock_state_manager.group_ismember.assert_any_call(IN_PROGRESS_FILES, "non_existent_file.xml")
    mock_state_manager.group_ismember.assert_any_call(PROCESSED_FILES, "non_existent_file.xml")
    mock_state_manager.group_add.assert_called_once_with(
        group_name=PENDING_FILES,
        values=[action_config.filename]
    )
    mock_file_storage.update_file_metadata.assert_called_once_with(
        integration_id=str(integration_v2.id),
        blob_name=action_config.filename,
        metadata={"status": FileStatus.PENDING.value}
    )
    assert result == {"file_status": "Not found", 'message': f"File '{action_config.filename}' not found in any group. Moving file to PENDING status."}


@pytest.mark.asyncio
async def test_action_set_file_status_exception(mocker, integration_v2, mock_state_manager, mock_file_storage):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    mock_state_manager.group_move.side_effect = Exception("Test exception")

    action_config = SetFileStatusConfig(filename="test_file.xml", status=FileStatus.IN_PROGRESS)

    result = await action_set_file_status(integration_v2, action_config)

    assert result == {"file_status": FileStatus.PENDING.value, "message": "Error setting file status"}


@pytest.mark.asyncio
async def test_action_reprocess_file(mocker, integration_v2, mock_file_storage, mock_state_manager):
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_process_data_file = mocker.patch("app.actions.handlers.process_data_file", new_callable=AsyncMock, return_value=10)
    action_config = ReprocessFileConfig(filename="test_file.xml")

    result = await action_reprocess_file(integration_v2, action_config)

    mock_process_data_file.assert_awaited_once_with(
        file_name="test_file.xml",
        integration=integration_v2,
        process_config=action_config
    )
    assert result == {"observations_processed": 10}


@pytest.mark.asyncio
async def test_action_reprocess_file_exception(mocker, integration_v2, mock_state_manager, mock_file_storage):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.file_storage", mock_file_storage)
    mock_state_manager.group_ismember.return_value = asyncio.Future()
    mock_state_manager.group_ismember.return_value.set_result(True)
    mock_state_manager.group_move.side_effect = Exception("Test exception")

    action_config = ReprocessFileConfig(filename="test_file.xml")

    result = await action_reprocess_file(integration_v2, action_config)

    assert result == {"observations_processed": 0, "message": "Reprocess for file 'test_file.xml' failed. Error: Test exception."}
