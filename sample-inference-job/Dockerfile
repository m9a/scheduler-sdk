FROM python:3.12-slim
RUN pip install --no-cache-dir websocket-client protobuf torch --extra-index-url https://download.pytorch.org/whl/cpu
COPY py-sdk/job_runner /opt/scheduler/job_runner
COPY py-sdk/scheduler /opt/scheduler/scheduler
ENV PYTHONPATH=/opt/scheduler
COPY sample-inference-job/inference_server.py /opt/job/inference_server.py
COPY sample-inference-job/serve_job.py /opt/job/serve_job.py
ENTRYPOINT ["python3", "-m", "job_runner", "/opt/job/serve_job.py"]
