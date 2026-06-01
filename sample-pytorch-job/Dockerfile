FROM python:3.12-slim
RUN pip install --no-cache-dir websocket-client protobuf torch --extra-index-url https://download.pytorch.org/whl/cpu
COPY py-sdk/job_runner /opt/scheduler/job_runner
COPY py-sdk/scheduler /opt/scheduler/scheduler
ENV PYTHONPATH=/opt/scheduler
COPY sample-pytorch-job/sine_wave_lstm.py /opt/job/sine_wave_lstm.py
ENTRYPOINT ["python3", "-m", "job_runner", "/opt/job/sine_wave_lstm.py"]
