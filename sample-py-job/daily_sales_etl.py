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
    def extract(self):
        print(f"Extracting sales data for region={self.region}")
        self.row_count = 5000

    @task("transform", order=2)
    def transform(self):
        print(f"Transforming {self.row_count} rows in batches of {self.batch_size}")

    @task("load", order=3)
    def load(self):
        print(f"Loading {self.row_count} rows into warehouse")

    @after_job
    def cleanup(self):
        print("Cleaning up temporary files")
