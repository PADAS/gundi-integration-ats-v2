import pytest

from app import settings
from app.services.file_storage import CloudFileStorage


@pytest.mark.asyncio
async def test_upload_file(mocker, mock_gcp_cloud_storage, integration_v2):
    mocker.patch("app.services.file_storage.Storage", mock_gcp_cloud_storage)
    file_storage = CloudFileStorage()
    integration_id = str(integration_v2.id)
    local_file = "test.xml"
    blob_name = "202412011002_points_dd65d9de-0ec8-480c-8719-c1f5ff4d639a.xml"
    metadata = {"ats_account": "marianom"}

    await file_storage.upload_file(
        integration_id=integration_id,
        local_file_path=local_file,
        destination_blob_name=blob_name,
        metadata=metadata
    )

    storage_client = mock_gcp_cloud_storage.return_value
    storage_client.upload_from_filename.assert_called_once_with(
        settings.GCP_BUCKET_NAME,
        f"integrations/{integration_id}/{blob_name}",
        local_file,
        metadata={"metadata": metadata}
    )


@pytest.mark.asyncio
async def test_download_file(mocker, mock_gcp_cloud_storage, integration_v2):
    mocker.patch("app.services.file_storage.Storage", mock_gcp_cloud_storage)
    file_storage = CloudFileStorage()
    integration_id = str(integration_v2.id)
    blob_name = "202412011002_points_dd65d9de-0ec8-480c-8719-c1f5ff4d639a.xml"

    await file_storage.download_file(
        integration_id=integration_id,
        source_blob_name=blob_name,
        destination_file_path=f"/tmp/{blob_name}"
    )

    storage_client = mock_gcp_cloud_storage.return_value
    storage_client.download_to_filename.assert_called_once_with(
        settings.GCP_BUCKET_NAME,
        f"integrations/{integration_id}/{blob_name}",
        f"/tmp/{blob_name}"
    )


@pytest.mark.asyncio
async def test_delete_file(mocker, mock_gcp_cloud_storage, integration_v2):
    mocker.patch("app.services.file_storage.Storage", mock_gcp_cloud_storage)
    file_storage = CloudFileStorage()
    integration_id = str(integration_v2.id)
    blob_name = "202412011002_points_dd65d9de-0ec8-480c-8719-c1f5ff4d639a.xml"

    await file_storage.delete_file(integration_id=integration_id, blob_name=blob_name)

    storage_client = mock_gcp_cloud_storage.return_value
    storage_client.delete.assert_called_once_with(
        settings.GCP_BUCKET_NAME,
        f"integrations/{integration_id}/{blob_name}"
    )


@pytest.mark.asyncio
async def test_list_files(mocker, mock_gcp_cloud_storage, integration_v2, gcp_bucket_list_response):
    mocker.patch("app.services.file_storage.Storage", mock_gcp_cloud_storage)
    file_storage = CloudFileStorage()
    integration_id = str(integration_v2.id)

    result = await file_storage.list_files(integration_id=integration_id)

    storage_client = mock_gcp_cloud_storage.return_value
    storage_client.list_objects.assert_called_once_with(
        settings.GCP_BUCKET_NAME,
        params={"prefix": f"integrations/{integration_id}"}
    )
    assert result == [blob["name"] for blob in gcp_bucket_list_response["items"]]


@pytest.mark.asyncio
async def test_get_file_metadata(mocker, mock_gcp_cloud_storage, integration_v2, get_gcp_file_metadata_response):
    mocker.patch("app.services.file_storage.Storage", mock_gcp_cloud_storage)
    file_storage = CloudFileStorage()
    integration_id = str(integration_v2.id)
    blob_name = "202412011002_points_dd65d9de-0ec8-480c-8719-c1f5ff4d639a.xml"

    result = await file_storage.get_file_metadata(integration_id=integration_id, blob_name=blob_name)

    storage_client = mock_gcp_cloud_storage.return_value
    storage_client.download_metadata.assert_called_once_with(
        settings.GCP_BUCKET_NAME,
        f"integrations/{integration_id}/{blob_name}"
    )
    assert result == get_gcp_file_metadata_response["metadata"]
