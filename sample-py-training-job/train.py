"""
MNIST classifier using PyTorch Lightning — demonstrates automatic metric
reporting with multiple tasks. Uses @job/@task for lifecycle, Lightning for
training. The SDK monkey-patches Trainer to inject MLFlowLogger, so
self.log() values flow to MLflow without any extra SDK imports or setup.
"""

import torch
import torch.nn.functional as F
import torchmetrics
from pytorch_lightning import LightningModule, LightningDataModule, Trainer
from torch.utils.data import DataLoader, random_split
from torchvision import datasets, transforms

from job_runner import job, task


class MnistModel(LightningModule):

    def __init__(self):
        super().__init__()
        self.conv1 = torch.nn.Conv2d(1, 16, 3, padding=1)
        self.conv2 = torch.nn.Conv2d(16, 32, 3, padding=1)
        self.fc = torch.nn.Linear(32 * 7 * 7, 10)
        self.accuracy = torchmetrics.Accuracy(task="multiclass", num_classes=10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2(x), 2))
        x = x.view(x.size(0), -1)
        return self.fc(x)

    def training_step(self, batch, batch_idx):
        x, y = batch
        loss = F.cross_entropy(self(x), y)
        self.log("train_loss", loss)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        preds = self(x)
        self.log("val_loss", F.cross_entropy(preds, y))
        self.log("val_acc", self.accuracy(preds, y))

    def test_step(self, batch, batch_idx):
        x, y = batch
        preds = self(x)
        self.log("test_loss", F.cross_entropy(preds, y))
        self.log("test_acc", self.accuracy(preds, y))

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=1e-3)


class MnistDataModule(LightningDataModule):

    def __init__(self, batch_size=64):
        super().__init__()
        self.batch_size = batch_size
        self.transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307,), (0.3081,)),
        ])

    def setup(self, stage=None):
        # Download to /tmp — /workspace/input is mounted read-only
        data_dir = "/tmp/mnist_data"
        full = datasets.MNIST(data_dir, train=True,
                              download=True, transform=self.transform)
        self.train_ds, self.val_ds = random_split(full, [55000, 5000])
        self.test_ds = datasets.MNIST(data_dir, train=False,
                                      download=True, transform=self.transform)

    def train_dataloader(self):
        return DataLoader(self.train_ds, batch_size=self.batch_size, shuffle=True)

    def val_dataloader(self):
        return DataLoader(self.val_ds, batch_size=self.batch_size)

    def test_dataloader(self):
        return DataLoader(self.test_ds, batch_size=self.batch_size)


@job(id="mnist-lightning", description="Train MNIST classifier with Lightning")
class MnistTrainingJob:

    def __init__(self, epochs: int = 3, batch_size: int = 64):
        self.epochs = int(epochs)
        self.batch_size = int(batch_size)
        self.model = None
        self.datamodule = None
        self.trainer = None

    @task("train", order=1)
    def train(self):
        self.model = MnistModel()
        self.datamodule = MnistDataModule(batch_size=self.batch_size)
        self.trainer = Trainer(max_epochs=self.epochs, accelerator="auto")
        self.trainer.fit(self.model, self.datamodule)

    @task("test", order=2)
    def test(self):
        self.trainer.test(self.model, self.datamodule)

    @task("export", order=3)
    def export(self):
        torch.save(self.model.state_dict(), "/workspace/output/model.pt")
        print("Model saved to /workspace/output/model.pt")
