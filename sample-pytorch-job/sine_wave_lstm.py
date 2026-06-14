import csv
import math
import time

import torch
import torch.nn as nn

from job_runner import job, task


class SineLSTM(nn.Module):
    def __init__(self, hidden_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size=1, hidden_size=hidden_size, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


@job(id="sine-wave-lstm", description="Train a tiny LSTM on sine wave data")
class SineWaveLstmJob:

    def __init__(self, epochs: int = 10, hidden_size: int = 32):
        self.epochs = int(epochs)
        self.hidden_size = int(hidden_size)
        self.x_train = None
        self.y_train = None

    @task("prepare_data", order=1)
    def prepare_data(self, ctx):
        values = []
        with open("/workspace/input/sine_data.csv") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                values.append(float(row[1]))

        window_size = 10
        xs, ys = [], []
        for i in range(len(values) - window_size):
            xs.append(values[i : i + window_size])
            ys.append(values[i + window_size])

        self.x_train = torch.tensor(xs, dtype=torch.float32).unsqueeze(-1)
        self.y_train = torch.tensor(ys, dtype=torch.float32).unsqueeze(-1)
        ctx.metric("training_sequences", len(xs))
        print(f"Prepared {len(xs)} training sequences (window={window_size})")

    @task("train_model", order=2)
    def train_model(self, ctx):
        model = SineLSTM(self.hidden_size)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        loss_fn = nn.MSELoss()

        # Raw PyTorch: the training loop reports its own progress per epoch.
        start = time.monotonic()
        for epoch in range(self.epochs):
            pred = model(self.x_train)
            loss = loss_fn(pred, self.y_train)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            ctx.progress(epoch + 1, self.epochs)
            ctx.metric("loss", loss.item())
            if (epoch + 1) % max(1, self.epochs // 5) == 0:
                print(f"Epoch {epoch + 1}/{self.epochs}, loss={loss.item():.6f}")
        ctx.metric("train_duration_ms", (time.monotonic() - start) * 1000)

        torch.save(model.state_dict(), "/workspace/output/model.pt")
        ctx.event("model_saved", "/workspace/output/model.pt")
        print("Model saved to /workspace/output/model.pt")
