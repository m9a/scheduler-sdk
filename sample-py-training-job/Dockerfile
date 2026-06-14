FROM pytorch/pytorch:2.3.1-cuda12.1-cudnn8-runtime
# mlflow client must match the server pinned in scheduler/docker-compose.test.yml
# (v2.16.0): a 3.x client calls /api/2.0/mlflow/logged-models, which a 2.x
# server 404s, killing autologging mid-run (no metrics get logged).
# pytorch-lightning is pinned to the top of mlflow 2.16's supported autolog
# range (1.9.0–2.4.0) — an unpinned install drifts out of range.
RUN pip install --no-cache-dir websocket-client protobuf "pytorch-lightning==2.4.0" torchmetrics torchvision "mlflow==2.16.0"
COPY py-sdk/job_runner /opt/scheduler/job_runner
COPY py-sdk/scheduler /opt/scheduler/scheduler
ENV PYTHONPATH=/opt/scheduler
COPY sample-py-training-job/train.py /opt/job/train.py
ENTRYPOINT ["python3", "-m", "job_runner", "/opt/job/train.py"]
