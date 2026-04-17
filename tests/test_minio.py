# tests/test_minio.py
"""
Tests for MinIO setup and tile uploader using mocked connections.
No live MinIO connection required.
"""
from unittest.mock import MagicMock, patch


class TestMinioSetup:
    """Tests for minio_setup functions."""

    def test_create_bucket_creates_when_not_exists(self):
        """create_bucket creates bucket when it does not exist."""
        from imagery.minio_setup import create_bucket

        client = MagicMock()
        client.bucket_exists.return_value = False

        create_bucket(client, "sentinel-tiles")
        client.make_bucket.assert_called_once_with("sentinel-tiles")

    def test_create_bucket_skips_when_exists(self):
        """create_bucket skips creation when bucket already exists."""
        from imagery.minio_setup import create_bucket

        client = MagicMock()
        client.bucket_exists.return_value = True

        create_bucket(client, "sentinel-tiles")
        client.make_bucket.assert_not_called()

    def test_get_client_uses_config(self):
        """get_client uses minio config values."""
        with patch("imagery.minio_setup.Minio") as mock_minio:
            from imagery.minio_setup import get_client

            get_client()
            assert mock_minio.called
            call_kwargs = mock_minio.call_args[1]
            assert call_kwargs["secure"] is False

    def test_create_bucket_checks_existence_first(self):
        """create_bucket checks if bucket exists before creating."""
        from imagery.minio_setup import create_bucket

        client = MagicMock()
        client.bucket_exists.return_value = False

        create_bucket(client, "sentinel-tiles")
        client.bucket_exists.assert_called_once_with("sentinel-tiles")


class TestTileUploader:
    """Tests for tile_uploader functions."""

    def test_upload_tile_calls_fput_object(self, tmp_path):
        """upload_tile calls fput_object with correct parameters."""
        from imagery.tile_uploader import upload_tile

        client = MagicMock()
        tile_path = tmp_path / "singapore_strait_20260409_B04.tif"
        tile_path.write_bytes(b"fake tif data")

        upload_tile(client, "sentinel-tiles", tile_path)
        assert client.fput_object.called

    def test_upload_tile_uses_date_prefix(self, tmp_path):
        """upload_tile organizes tiles under date prefix."""
        from imagery.tile_uploader import upload_tile

        client = MagicMock()
        tile_path = tmp_path / "singapore_strait_20260409_B04.tif"
        tile_path.write_bytes(b"fake tif data")

        key = upload_tile(client, "sentinel-tiles", tile_path)
        assert key.startswith("20260409/")

    def test_upload_tile_returns_object_key(self, tmp_path):
        """upload_tile returns the object key."""
        from imagery.tile_uploader import upload_tile

        client = MagicMock()
        tile_path = tmp_path / "singapore_strait_20260409_B08.tif"
        tile_path.write_bytes(b"fake tif data")

        key = upload_tile(client, "sentinel-tiles", tile_path)
        assert "singapore_strait_20260409_B08.tif" in key

    def test_upload_all_skips_empty_directory(self, tmp_path):
        """upload_all returns empty list when no tiles found."""
        with patch("imagery.tile_uploader.PROCESSED_DIR", tmp_path):
            with patch("imagery.tile_uploader.get_client") as mock_client:
                from imagery.tile_uploader import upload_all

                result = upload_all()
                assert result == []

    def test_upload_all_uploads_all_tifs(self, tmp_path):
        """upload_all uploads all .tif files in processed directory."""
        for name in ["tile_20260409_B04.tif", "tile_20260409_B08.tif"]:
            (tmp_path / name).write_bytes(b"fake tif data")

        with patch("imagery.tile_uploader.PROCESSED_DIR", tmp_path):
            with patch("imagery.tile_uploader.get_client") as mock_get_client:
                mock_client = MagicMock()
                mock_get_client.return_value = mock_client
                mock_client.fput_object.return_value = None

                from imagery.tile_uploader import upload_all

                result = upload_all()
                assert len(result) == 2

    def test_upload_all_filters_by_date(self, tmp_path):
        """upload_all filters tiles by date when date_filter provided."""
        for name in ["tile_20260409_B04.tif", "tile_20260411_B04.tif"]:
            (tmp_path / name).write_bytes(b"fake tif data")

        with patch("imagery.tile_uploader.PROCESSED_DIR", tmp_path):
            with patch("imagery.tile_uploader.get_client") as mock_get_client:
                mock_client = MagicMock()
                mock_get_client.return_value = mock_client
                mock_client.fput_object.return_value = None

                from imagery.tile_uploader import upload_all

                result = upload_all(date_filter="20260409")
                assert len(result) == 1


class TestMinioSetupMain:
    """Tests for minio_setup main function."""

    def test_main_calls_create_bucket(self):
        """main calls create_bucket with configured bucket name."""
        with patch("imagery.minio_setup.get_client") as mock_get_client, patch(
            "imagery.minio_setup.create_bucket"
        ) as mock_create:

            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            from imagery.minio_setup import main

            main()

            assert mock_create.called

    def test_main_handles_s3_error(self):
        """main handles S3Error gracefully."""
        from minio.error import S3Error

        with patch("imagery.minio_setup.get_client") as mock_get_client, patch(
            "imagery.minio_setup.create_bucket"
        ) as mock_create:

            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_create.side_effect = S3Error(
                "BucketAlreadyExists",
                "bucket exists",
                "sentinel-tiles",
                "req-id",
                "host-id",
                MagicMock(),
            )

            from imagery.minio_setup import main

            main()
