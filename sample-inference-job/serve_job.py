"""
Job wrapper for the inference server. The single task blocks on
subprocess.run while the server handles HTTP requests. When /shutdown
is called, the subprocess exits 0 and the task completes normally.
"""

import subprocess

from job_runner import job, task


@job(id="sine-wave-inference", description="Serve LSTM inference over HTTP")
class SineWaveInferenceJob:

    def __init__(self, hidden_size: int = 32, port: int = 8080):
        self.hidden_size = int(hidden_size)
        self.port = int(port)

    @task("serve", order=1)
    def serve(self, ctx):
        subprocess.run([
            "python3", "/opt/job/inference_server.py",
            "--model", "/workspace/input/model.pt",
            "--hidden-size", str(self.hidden_size),
            "--port", str(self.port),
        ], check=True)
