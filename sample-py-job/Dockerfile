FROM python:3.12-slim
RUN pip install --no-cache-dir websocket-client protobuf
COPY py-sdk/job_runner /opt/scheduler/job_runner
COPY py-sdk/scheduler /opt/scheduler/scheduler
ENV PYTHONPATH=/opt/scheduler
COPY sample-py-job/daily_sales_etl.py /opt/job/daily_sales_etl.py
ENTRYPOINT ["python3", "-m", "job_runner", "/opt/job/daily_sales_etl.py"]
