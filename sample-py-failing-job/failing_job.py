from job_runner import job, task


@job(id="failing-job", description="Job with a task that deliberately fails")
class FailingJob:

    @task("setup_data", order=1)
    def setup_data(self, ctx):
        print("Setting up data — this task succeeds")

    @task("process", order=2)
    def process(self, ctx):
        raise RuntimeError("Intentional failure for testing")

    @task("finalize", order=3)
    def finalize(self, ctx):
        print("This task should never run")
