FROM pytorch/pytorch:2.3.1-cuda12.1-cudnn8-runtime
RUN pip install --no-cache-dir websocket-client protobuf pytorch-lightning torchmetrics torchvision mlflow
COPY py-sdk/job_runner /opt/scheduler/job_runner
COPY py-sdk/scheduler /opt/scheduler/scheduler
ENV PYTHONPATH=/opt/scheduler
COPY sample-py-failing-training-job/train.py /opt/job/train.py
ENTRYPOINT ["python3", "-m", "job_runner", "/opt/job/train.py"]
