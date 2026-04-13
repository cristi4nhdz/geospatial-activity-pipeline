# imagery/patch_classifier.py
"""
Patch Classifier Module

Defines and trains a lightweight PyTorch binary CNN classifier
that scores 512x512 image patches as anomalous or normal.
The model is trained on synthetic patches derived from the
NDVI delta arrays computed during change detection.
"""
import logging
from pathlib import Path
import numpy as np
import torch
from torch import nn
from torch import optim
from torch.utils.data import DataLoader, Dataset
from config.config_loader import config
from config.logging_config import setup_logging
from imagery.change_detection import (
    compute_ndvi,
    download_band,
    get_client,
    list_dates,
)

setup_logging("patch_classifier.log")
logger = logging.getLogger(__name__)

WEIGHTS_DIR = Path("imagery/weights")
WEIGHTS_PATH = WEIGHTS_DIR / "patch_classifier.pt"


class PatchCNN(nn.Module):
    """
    Lightweight binary CNN for anomaly patch classification.

    Takes a single-channel 512x512 NDVI delta patch and outputs
    a probability score between 0 and 1.
    """

    def __init__(self) -> None:
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(1, 16, kernel_size=3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(16, 32, kernel_size=3, padding=1),
            nn.ReLU(),
            nn.MaxPool2d(2),
            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d((4, 4)),
        )
        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(64 * 4 * 4, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 1),
            nn.Sigmoid(),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """
        Forward pass through the network.

        Args:
            x: Input tensor of shape (batch, 1, 512, 512).

        Returns:
            torch.Tensor: Anomaly probability scores of shape (batch, 1).
        """
        return self.classifier(self.features(x))


class PatchDataset(Dataset):
    """
    Synthetic patch dataset built from NDVI delta arrays.

    Positive samples (anomaly) are patches with mean delta above threshold.
    Negative samples (normal) are patches with mean delta below threshold.
    """

    def __init__(
        self,
        patches: list[np.ndarray],
        labels: list[int],
        patch_size: int = 512,
    ) -> None:
        self.patches = patches
        self.labels = labels
        self.patch_size = patch_size

    def __len__(self) -> int:
        return len(self.patches)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        patch = self.patches[idx]
        patch = np.resize(patch, (self.patch_size, self.patch_size))
        patch_tensor = torch.tensor(patch, dtype=torch.float32).unsqueeze(0)
        label_tensor = torch.tensor([self.labels[idx]], dtype=torch.float32)
        return patch_tensor, label_tensor


def build_dataset(
    ndvi_delta: np.ndarray,
    patch_size: int,
    threshold: float,
) -> PatchDataset:
    """
    Build a PatchDataset from an NDVI delta array.

    Args:
        ndvi_delta: 2D array of absolute NDVI deltas.
        patch_size: Size of each patch in pixels.
        threshold: NDVI delta threshold for positive labeling.

    Returns:
        PatchDataset with patches and binary labels.
    """
    patches = []
    labels = []
    rows, cols = ndvi_delta.shape

    for row in range(0, rows, patch_size):
        for col in range(0, cols, patch_size):
            patch = ndvi_delta[row : row + patch_size, col : col + patch_size]
            mean_delta = float(np.mean(patch))
            patches.append(patch)
            labels.append(1 if mean_delta >= threshold else 0)

    logger.info(
        "Built dataset: %s patches (%s positive, %s negative)",
        len(patches),
        sum(labels),
        len(labels) - sum(labels),
    )
    return PatchDataset(patches, labels, patch_size)


def train(
    model: PatchCNN,
    dataset: PatchDataset,
    epochs: int = 10,
    lr: float = 1e-3,
) -> PatchCNN:
    """
    Train the patch classifier on the provided dataset.

    Args:
        model: PatchCNN model instance.
        dataset: PatchDataset to train on.
        epochs: Number of training epochs.
        lr: Learning rate.

    Returns:
        Trained PatchCNN model.
    """
    loader = DataLoader(dataset, batch_size=4, shuffle=True)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)

    model.train()
    for epoch in range(epochs):
        total_loss = 0.0
        for patches, labels in loader:
            optimizer.zero_grad()
            outputs = model(patches)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()

        avg_loss = total_loss / len(loader)
        logger.info("Epoch %s/%s | Loss: %.4f", epoch + 1, epochs, avg_loss)

    return model


def save_model(model: PatchCNN) -> None:
    """
    Save model weights to disk.

    Args:
        model: Trained PatchCNN model.
    """
    WEIGHTS_DIR.mkdir(parents=True, exist_ok=True)
    torch.save(model.state_dict(), WEIGHTS_PATH)
    logger.info("Model saved to %s", WEIGHTS_PATH)


def load_model() -> PatchCNN:
    """
    Load a trained PatchCNN model from disk.

    Returns:
        PatchCNN model with loaded weights.
    """
    model = PatchCNN()
    model.load_state_dict(torch.load(WEIGHTS_PATH, weights_only=True))
    model.eval()
    logger.info("Model loaded from %s", WEIGHTS_PATH)
    return model


def score_patch(model: PatchCNN, patch: np.ndarray) -> float:
    """
    Score a single patch using the trained model.

    Args:
        model: Trained PatchCNN model.
        patch: 2D numpy array patch.

    Returns:
        Anomaly probability score between 0 and 1.
    """
    patch = np.resize(patch, (512, 512))
    tensor = torch.tensor(patch, dtype=torch.float32).unsqueeze(0).unsqueeze(0)
    with torch.no_grad():
        score = model(tensor).item()
    return score


def main() -> None:
    """
    Train the patch classifier on the most recent NDVI delta
    and save weights to imagery/weights/patch_classifier.pt
    """

    cd_config = config["change_detection"]
    client = get_client()
    bucket = config["minio"]["bucket"]
    dates = list_dates(client, bucket)

    if len(dates) < 2:
        logger.error("Need at least 2 tile dates to train - run change detection first")
        return

    date_old, date_new = dates[-2], dates[-1]

    b04_old = download_band(client, bucket, date_old, "B04")
    b08_old = download_band(client, bucket, date_old, "B08")
    b04_new = download_band(client, bucket, date_new, "B04")
    b08_new = download_band(client, bucket, date_new, "B08")

    ndvi_old = compute_ndvi(b04_old, b08_old)
    ndvi_new = compute_ndvi(b04_new, b08_new)
    ndvi_delta = np.abs(ndvi_new - ndvi_old)

    dataset = build_dataset(
        ndvi_delta,
        patch_size=cd_config["patch_size"],
        threshold=cd_config["ndvi_threshold"],
    )

    model = PatchCNN()
    logger.info("Training patch classifier")
    model = train(model, dataset, epochs=10)
    save_model(model)
    logger.info("Training complete")


if __name__ == "__main__":
    main()
