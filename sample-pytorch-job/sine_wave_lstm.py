import csv
import math

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
    def prepare_data(self):
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
        print(f"Prepared {len(xs)} training sequences (window={window_size})")

    @task("train_model", order=2)
    def train_model(self):
        model = SineLSTM(self.hidden_size)
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        loss_fn = nn.MSELoss()

        for epoch in range(self.epochs):
            pred = model(self.x_train)
            loss = loss_fn(pred, self.y_train)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            if (epoch + 1) % max(1, self.epochs // 5) == 0:
                print(f"Epoch {epoch + 1}/{self.epochs}, loss={loss.item():.6f}")

        torch.save(model.state_dict(), "/workspace/output/model.pt")
        print("Model saved to /workspace/output/model.pt")
