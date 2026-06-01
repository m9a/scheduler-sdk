"""
Standalone HTTP inference server for a trained SineLSTM model.

CLI args: --model <path> --hidden-size <int> --port <int>

Endpoints:
  GET  /health   → 200
  POST /predict  → {"sequence": [float...]} → {"prediction": float}
  POST /shutdown → triggers clean shutdown, responds 200
"""

import argparse
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import torch
import torch.nn as nn


class SineLSTM(nn.Module):
    def __init__(self, hidden_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size=1, hidden_size=hidden_size, batch_first=True)
        self.fc = nn.Linear(hidden_size, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


OUTPUT_FILE = "/workspace/output/predictions.jsonl"

model: SineLSTM | None = None
server: HTTPServer | None = None


class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/predict":
            self._handle_predict()
        elif self.path == "/shutdown":
            self._handle_shutdown()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_predict(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))
        sequence = body["sequence"]

        tensor = torch.tensor([sequence], dtype=torch.float32).unsqueeze(-1)
        with torch.no_grad():
            prediction = model(tensor).item()

        result = {"sequence": sequence, "prediction": prediction}
        with open(OUTPUT_FILE, "a") as f:
            f.write(json.dumps(result) + "\n")

        response = json.dumps({"prediction": prediction})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(response.encode())

    def _handle_shutdown(self):
        self.send_response(200)
        self.end_headers()
        # Shut down on a separate thread so the response completes first
        threading.Thread(target=server.shutdown, daemon=True).start()

    def log_message(self, format, *args):
        print(f"[inference_server] {args[0]}")


def main():
    global model, server

    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--hidden-size", type=int, default=32)
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    model = SineLSTM(args.hidden_size)
    model.load_state_dict(torch.load(args.model, weights_only=True))
    model.eval()
    print(f"[inference_server] Loaded model from {args.model} (hidden_size={args.hidden_size})")

    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"[inference_server] Serving on port {args.port}")
    server.serve_forever()
    print("[inference_server] Server stopped")


if __name__ == "__main__":
    main()
