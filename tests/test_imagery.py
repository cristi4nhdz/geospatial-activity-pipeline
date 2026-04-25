# tests/test_imagery.py
"""
Tests for imagery pipeline pure functions.
No live connections required, tests numpy and PyTorch logic only.
"""
import numpy as np
import pytest
from unittest.mock import MagicMock, patch


class TestComputeNDVI:
    """Tests for NDVI computation."""

    def test_ndvi_basic_calculation(self, tmp_path):
        """NDVI computed correctly from B04 and B08 arrays."""
        import rasterio
        from rasterio.transform import from_bounds
        from imagery.change_detection import compute_ndvi
        import os
        import sys

        os.environ["GDAL_DATA"] = sys.prefix + "/Library/share/gdal"
        os.environ["GDAL_DRIVER_PATH"] = sys.prefix + "/Library/lib/gdalplugins"

        transform = from_bounds(103.5, 1.0, 104.5, 1.6, 100, 100)
        b04_path = tmp_path / "b04.tif"
        b08_path = tmp_path / "b08.tif"

        b04_data = np.full((1, 100, 100), 1000.0, dtype=np.float32)
        b08_data = np.full((1, 100, 100), 3000.0, dtype=np.float32)

        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": 100,
            "height": 100,
            "count": 1,
            "crs": "EPSG:4326",
            "transform": transform,
        }

        with rasterio.open(b04_path, "w", **profile) as dst:
            dst.write(b04_data)
        with rasterio.open(b08_path, "w", **profile) as dst:
            dst.write(b08_data)

        ndvi, _ = compute_ndvi(b04_path, b08_path)
        assert ndvi.shape == (100, 100)
        expected = (3000 - 1000) / (3000 + 1000)
        assert np.allclose(ndvi, expected, atol=0.001)

    def test_ndvi_zero_division_handled(self, tmp_path):
        """NDVI handles zero division when B04 + B08 = 0."""
        import rasterio
        from rasterio.transform import from_bounds
        from imagery.change_detection import compute_ndvi
        import os
        import sys

        os.environ["GDAL_DATA"] = sys.prefix + "/Library/share/gdal"
        os.environ["GDAL_DRIVER_PATH"] = sys.prefix + "/Library/lib/gdalplugins"

        transform = from_bounds(103.5, 1.0, 104.5, 1.6, 10, 10)
        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": 10,
            "height": 10,
            "count": 1,
            "crs": "EPSG:4326",
            "transform": transform,
        }

        b04_path = tmp_path / "b04.tif"
        b08_path = tmp_path / "b08.tif"

        with rasterio.open(b04_path, "w", **profile) as dst:
            dst.write(np.zeros((1, 10, 10), dtype=np.float32))
        with rasterio.open(b08_path, "w", **profile) as dst:
            dst.write(np.zeros((1, 10, 10), dtype=np.float32))

        ndvi, _ = compute_ndvi(b04_path, b08_path)
        assert not np.any(np.isnan(ndvi))
        assert np.all(ndvi == 0)

    def test_ndvi_range(self, tmp_path):
        """NDVI values are in valid range -1 to 1."""
        import rasterio
        from rasterio.transform import from_bounds
        from imagery.change_detection import compute_ndvi
        import os
        import sys

        os.environ["GDAL_DATA"] = sys.prefix + "/Library/share/gdal"
        os.environ["GDAL_DRIVER_PATH"] = sys.prefix + "/Library/lib/gdalplugins"

        transform = from_bounds(103.5, 1.0, 104.5, 1.6, 50, 50)
        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": 50,
            "height": 50,
            "count": 1,
            "crs": "EPSG:4326",
            "transform": transform,
        }

        b04_path = tmp_path / "b04.tif"
        b08_path = tmp_path / "b08.tif"

        rng = np.random.default_rng(42)
        with rasterio.open(b04_path, "w", **profile) as dst:
            dst.write(rng.integers(0, 5000, (1, 50, 50)).astype(np.float32))
        with rasterio.open(b08_path, "w", **profile) as dst:
            dst.write(rng.integers(0, 5000, (1, 50, 50)).astype(np.float32))

        ndvi, _ = compute_ndvi(b04_path, b08_path)
        assert np.all(ndvi >= -1.0)
        assert np.all(ndvi <= 1.0)


class TestDetectAnomalies:
    """Tests for anomaly patch detection."""

    def test_detects_anomaly_above_threshold(self):
        """Patches above threshold are flagged as anomalies."""
        from imagery.change_detection import detect_anomalies

        ndvi_old = np.zeros((512, 512))
        ndvi_new = np.full((512, 512), 0.5)

        anomalies = detect_anomalies(ndvi_old, ndvi_new, patch_size=512, threshold=0.15)
        assert len(anomalies) == 1
        assert anomalies[0]["mean_delta"] > 0.15

    def test_no_anomaly_below_threshold(self):
        """Patches below threshold are not flagged."""
        from imagery.change_detection import detect_anomalies

        ndvi_old = np.zeros((512, 512))
        ndvi_new = np.full((512, 512), 0.05)

        anomalies = detect_anomalies(ndvi_old, ndvi_new, patch_size=512, threshold=0.15)
        assert len(anomalies) == 0

    def test_anomaly_has_required_fields(self):
        """Anomaly dict has all required fields."""
        from imagery.change_detection import detect_anomalies

        ndvi_old = np.zeros((512, 512))
        ndvi_new = np.full((512, 512), 0.5)

        anomalies = detect_anomalies(ndvi_old, ndvi_new, patch_size=512, threshold=0.15)
        assert len(anomalies) > 0
        required = {"row", "col", "patch_size", "mean_delta", "max_delta"}
        assert required.issubset(anomalies[0].keys())

    def test_multiple_patches_detected(self):
        """Multiple anomaly patches detected in large array."""
        from imagery.change_detection import detect_anomalies

        ndvi_old = np.zeros((1024, 1024))
        ndvi_new = np.full((1024, 1024), 0.5)

        anomalies = detect_anomalies(ndvi_old, ndvi_new, patch_size=512, threshold=0.15)
        assert len(anomalies) == 4

    def test_patch_coordinates_correct(self):
        """Anomaly patch row/col coordinates are correct."""
        from imagery.change_detection import detect_anomalies

        ndvi_old = np.zeros((512, 512))
        ndvi_new = np.full((512, 512), 0.5)

        anomalies = detect_anomalies(ndvi_old, ndvi_new, patch_size=512, threshold=0.15)
        assert anomalies[0]["row"] == 0
        assert anomalies[0]["col"] == 0
        assert anomalies[0]["patch_size"] == 512


class TestPatchCNN:
    """Tests for PyTorch patch classifier."""

    def test_model_instantiates(self):
        """PatchCNN model instantiates without errors."""
        from imagery.patch_classifier import PatchCNN

        model = PatchCNN()
        assert model is not None

    def test_forward_pass_shape(self):
        """Forward pass returns correct output shape."""
        import torch
        from imagery.patch_classifier import PatchCNN

        model = PatchCNN()
        model.eval()
        x = torch.zeros(1, 1, 512, 512)
        with torch.no_grad():
            output = model(x)
        assert output.shape == (1, 1)

    def test_output_is_probability(self):
        """Forward pass output is between 0 and 1."""
        import torch
        from imagery.patch_classifier import PatchCNN

        model = PatchCNN()
        model.eval()
        x = torch.rand(2, 1, 512, 512)
        with torch.no_grad():
            output = model(x)
        assert torch.all(output >= 0)
        assert torch.all(output <= 1)

    def test_build_dataset(self):
        """PatchDataset builds correctly from NDVI delta."""
        from imagery.patch_classifier import build_dataset

        ndvi_delta = np.random.rand(1024, 1024).astype(np.float32)
        dataset = build_dataset(ndvi_delta, patch_size=512, threshold=0.5)

        assert len(dataset) == 4
        assert all(label in [0, 1] for label in dataset.labels)

    def test_dataset_item_shape(self):
        """Dataset items have correct tensor shape."""
        from imagery.patch_classifier import build_dataset

        ndvi_delta = np.ones((512, 512), dtype=np.float32) * 0.8
        dataset = build_dataset(ndvi_delta, patch_size=512, threshold=0.5)

        patch, label = dataset[0]
        assert patch.shape == (1, 512, 512)
        assert label.shape == (1,)

    def test_score_patch_returns_float(self):
        """score_patch returns a float between 0 and 1."""
        from imagery.patch_classifier import PatchCNN, score_patch

        model = PatchCNN()
        model.eval()
        patch = np.random.rand(512, 512).astype(np.float32)
        score = score_patch(model, patch)

        assert isinstance(score, float)
        assert 0.0 <= score <= 1.0


class TestAnomalyScorer:
    """Tests for anomaly scoring logic."""

    def test_score_anomalies_returns_list(self):
        """score_anomalies returns a list of scored events."""
        from imagery.anomaly_scorer import score_anomalies
        from imagery.patch_classifier import PatchCNN

        model = PatchCNN()
        model.eval()

        ndvi_delta = np.full((512, 512), 0.3, dtype=np.float32)
        anomalies = [
            {"row": 0, "col": 0, "patch_size": 512, "mean_delta": 0.3, "max_delta": 0.5}
        ]

        scored = score_anomalies(anomalies, ndvi_delta, model, patch_size=512)
        assert len(scored) == 1

    def test_scored_event_has_required_fields(self):
        """Scored event has all required fields."""
        from imagery.anomaly_scorer import score_anomalies
        from imagery.patch_classifier import PatchCNN

        model = PatchCNN()
        model.eval()

        ndvi_delta = np.full((512, 512), 0.3, dtype=np.float32)
        anomalies = [
            {"row": 0, "col": 0, "patch_size": 512, "mean_delta": 0.3, "max_delta": 0.5}
        ]

        scored = score_anomalies(anomalies, ndvi_delta, model, patch_size=512)
        required = {
            "row",
            "col",
            "patch_size",
            "mean_delta",
            "max_delta",
            "ndvi_score",
            "cnn_score",
            "confidence",
            "detected_at",
        }
        assert required.issubset(scored[0].keys())

    def test_confidence_is_average(self):
        """Confidence is average of ndvi_score and cnn_score."""
        from imagery.anomaly_scorer import score_anomalies
        from imagery.patch_classifier import PatchCNN

        model = PatchCNN()
        model.eval()

        ndvi_delta = np.full((512, 512), 0.3, dtype=np.float32)
        anomalies = [
            {"row": 0, "col": 0, "patch_size": 512, "mean_delta": 0.3, "max_delta": 0.5}
        ]

        scored = score_anomalies(anomalies, ndvi_delta, model, patch_size=512)
        event = scored[0]
        expected_confidence = round((event["ndvi_score"] + event["cnn_score"]) / 2, 4)
        assert event["confidence"] == pytest.approx(expected_confidence, abs=0.001)

    def test_results_sorted_by_confidence(self):
        """Results are sorted by confidence descending."""
        from imagery.anomaly_scorer import score_anomalies
        from imagery.patch_classifier import PatchCNN

        model = PatchCNN()
        model.eval()

        ndvi_delta = np.random.rand(1024, 512).astype(np.float32)
        anomalies = [
            {
                "row": 0,
                "col": 0,
                "patch_size": 512,
                "mean_delta": 0.3,
                "max_delta": 0.5,
            },
            {
                "row": 512,
                "col": 0,
                "patch_size": 512,
                "mean_delta": 0.6,
                "max_delta": 0.9,
            },
        ]

        scored = score_anomalies(anomalies, ndvi_delta, model, patch_size=512)
        confidences = [e["confidence"] for e in scored]
        assert confidences == sorted(confidences, reverse=True)


class TestAnomalyScorerSaveEvents:
    """Tests for save_events function."""

    def test_save_events_creates_file(self, tmp_path):
        """save_events writes JSON file to events directory."""
        with patch("imagery.anomaly_scorer.EVENTS_DIR", tmp_path):
            from imagery.anomaly_scorer import save_events

            events = [
                {
                    "row": 0,
                    "col": 0,
                    "confidence": 0.7,
                    "detected_at": "2026-04-13T13:41:38+00:00",
                }
            ]

            path = save_events(events, "20260409", "20260411")
            assert path.exists()

    def test_save_events_correct_filename(self, tmp_path):
        """save_events uses correct filename format."""
        with patch("imagery.anomaly_scorer.EVENTS_DIR", tmp_path):
            from imagery.anomaly_scorer import save_events

            events = [
                {
                    "row": 0,
                    "col": 0,
                    "confidence": 0.7,
                    "detected_at": "2026-04-13T13:41:38+00:00",
                }
            ]
            path = save_events(events, "20260409", "20260411")
            assert "20260409_vs_20260411" in path.name

    def test_save_events_valid_json(self, tmp_path):
        """save_events writes valid JSON."""
        import json

        with patch("imagery.anomaly_scorer.EVENTS_DIR", tmp_path):
            from imagery.anomaly_scorer import save_events

            events = [
                {
                    "row": 0,
                    "col": 0,
                    "confidence": 0.7,
                    "detected_at": "2026-04-13T13:41:38+00:00",
                }
            ]
            path = save_events(events, "20260409", "20260411")

            with open(path, encoding="utf-8") as f:
                loaded = json.load(f)
            assert len(loaded) == 1
            assert loaded[0]["confidence"] == 0.7


class TestChangeDetectionMinio:
    """Tests for MinIO operations in change_detection."""

    def test_list_dates_returns_sorted_list(self):
        """list_dates returns sorted list of date strings."""
        with patch("imagery.change_detection.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            obj1 = MagicMock()
            obj1.object_name = "20260409/tile.tif"
            obj2 = MagicMock()
            obj2.object_name = "20260411/tile.tif"
            mock_client.list_objects.return_value = [obj2, obj1]

            from imagery.change_detection import list_dates

            dates = list_dates(mock_client, "sentinel-tiles")

            assert dates == ["20260409", "20260411"]

    def test_download_band_calls_fget_object(self, tmp_path):
        """download_band calls fget_object with correct key."""
        with patch("imagery.change_detection.TEMP_DIR", tmp_path):
            mock_client = MagicMock()

            from imagery.change_detection import download_band

            path = download_band(mock_client, "sentinel-tiles", "20260409", "B04")

            assert mock_client.fget_object.called
            call_args = mock_client.fget_object.call_args[0]
            assert "20260409" in call_args[1]
            assert "B04" in call_args[1]

    def test_download_band_returns_path(self, tmp_path):
        """download_band returns a Path object."""
        with patch("imagery.change_detection.TEMP_DIR", tmp_path):
            mock_client = MagicMock()

            from imagery.change_detection import download_band
            from pathlib import Path

            result = download_band(mock_client, "sentinel-tiles", "20260409", "B04")

            assert isinstance(result, Path)


class TestPatchClassifierSaveLoad:
    """Tests for model save and load functions."""

    def test_save_model_creates_file(self, tmp_path):
        """save_model writes weights file to disk."""
        with patch("imagery.patch_classifier.WEIGHTS_DIR", tmp_path), patch(
            "imagery.patch_classifier.WEIGHTS_PATH", tmp_path / "patch_classifier.pt"
        ):
            from imagery.patch_classifier import PatchCNN, save_model

            model = PatchCNN()
            save_model(model)
            assert (tmp_path / "patch_classifier.pt").exists()

    def test_load_model_returns_patchcnn(self, tmp_path):
        """load_model returns a PatchCNN instance."""
        weights_path = tmp_path / "patch_classifier.pt"
        with patch("imagery.patch_classifier.WEIGHTS_DIR", tmp_path), patch(
            "imagery.patch_classifier.WEIGHTS_PATH", weights_path
        ):
            from imagery.patch_classifier import PatchCNN, save_model, load_model

            model = PatchCNN()
            save_model(model)
            loaded = load_model()
            assert isinstance(loaded, PatchCNN)

    def test_train_reduces_loss(self):
        """train runs without errors and returns trained model."""
        import numpy as np
        from imagery.patch_classifier import PatchCNN, build_dataset, train

        ndvi_delta = np.random.rand(1024, 1024).astype(np.float32)
        dataset = build_dataset(ndvi_delta, patch_size=512, threshold=0.5)
        model = PatchCNN()
        trained = train(model, dataset, epochs=2)
        assert trained is not None


class TestAnomalyScorerRun:
    """Tests for anomaly_scorer run function."""

    def test_run_returns_empty_when_no_anomalies(self, tmp_path):
        """run returns empty list when no anomalies detected."""
        import rasterio
        from rasterio.transform import from_bounds
        import os
        import sys

        os.environ["GDAL_DATA"] = sys.prefix + "/Library/share/gdal"
        os.environ["GDAL_DRIVER_PATH"] = sys.prefix + "/Library/lib/gdalplugins"

        with patch("imagery.anomaly_scorer.get_client") as mock_get_client, patch(
            "imagery.anomaly_scorer.EVENTS_DIR", tmp_path
        ):

            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            transform = from_bounds(103.5, 1.0, 104.5, 1.6, 512, 512)
            profile = {
                "driver": "GTiff",
                "dtype": "float32",
                "width": 512,
                "height": 512,
                "count": 1,
                "crs": "EPSG:4326",
                "transform": transform,
            }

            b04_path = tmp_path / "b04.tif"
            b08_path = tmp_path / "b08.tif"
            with rasterio.open(b04_path, "w", **profile) as dst:
                dst.write(np.zeros((1, 512, 512), dtype=np.float32))
            with rasterio.open(b08_path, "w", **profile) as dst:
                dst.write(np.zeros((1, 512, 512), dtype=np.float32))

            def mock_download(client, bucket, date, band):
                return b04_path if band == "B04" else b08_path

            with patch(
                "imagery.anomaly_scorer.download_band", side_effect=mock_download
            ), patch("imagery.anomaly_scorer.load_model") as mock_load_model:
                from imagery.patch_classifier import PatchCNN

                mock_load_model.return_value = PatchCNN()

                from imagery.anomaly_scorer import run

                result = run("20260409", "20260411")
                assert result == []


class TestChangeDetectionMain:
    """Tests for change_detection main function."""

    def test_main_warns_when_insufficient_dates(self):
        """main warns when fewer than 2 dates available."""
        with patch("imagery.change_detection.get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            obj = MagicMock()
            obj.object_name = "20260409/tile.tif"
            mock_client.list_objects.return_value = [obj]

            from imagery.change_detection import main

            main()

    def test_main_runs_with_two_dates(self, tmp_path):
        """main runs change detection when two dates available."""
        with patch("imagery.change_detection.get_client") as mock_get_client, patch(
            "imagery.change_detection.run"
        ) as mock_run:

            mock_client = MagicMock()
            mock_get_client.return_value = mock_client

            obj1 = MagicMock()
            obj1.object_name = "20260409/tile.tif"
            obj2 = MagicMock()
            obj2.object_name = "20260411/tile.tif"
            mock_client.list_objects.return_value = [obj1, obj2]
            mock_run.return_value = []

            from imagery.change_detection import main

            main()
            mock_run.assert_called_once_with("20260409", "20260411")
