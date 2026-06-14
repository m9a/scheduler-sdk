from job_runner import job, task, before_job, after_job


@job(id="daily-sales-etl", description="Daily sales ETL pipeline")
class DailySalesEtlJob:

    def __init__(self, region: str, batch_size: int = 1000):
        self.region = region
        self.batch_size = int(batch_size)
        self.row_count = 0

    @before_job
    def setup(self):
        print(f"Setting up ETL for region={self.region}, batch_size={self.batch_size}")

    @task("extract", order=1)
    def extract(self, ctx):
        print(f"Extracting sales data for region={self.region}")
        self.row_count = 5000
        ctx.metric("rows_extracted", self.row_count)

    @task("transform", order=2)
    def transform(self, ctx):
        import time
        print(f"Transforming {self.row_count} rows in batches of {self.batch_size}")
        start = time.monotonic()
        num_batches = (self.row_count + self.batch_size - 1) // self.batch_size
        for i in range(num_batches):
            ctx.progress(i + 1, num_batches)
        ctx.metric("transform_duration_ms", (time.monotonic() - start) * 1000)

    @task("load", order=3)
    def load(self, ctx):
        print(f"Loading {self.row_count} rows into warehouse")
        ctx.event("load_complete", f"{self.row_count} rows")

    @after_job
    def cleanup(self):
        print("Cleaning up temporary files")
