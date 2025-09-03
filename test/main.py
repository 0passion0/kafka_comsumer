import pathway as pw
import time as tt

class PrintBatchObserver(pw.io.python.ConnectorObserver):
    """
    A simple observer to collect data into batches and print them.

    This helps in verifying that the data pipeline is receiving data
    without flooding the console.
    """
    def __init__(self, batch_size=20, time_window=60):
        super().__init__()
        self.batch = []
        self.batch_size = batch_size
        self.time_window = time_window
        self.last_batch_time = tt.time()

    def on_change(self, key, row, time: int, is_addition: bool):
        """Called for each new row of data."""
        self.batch.append(row)
        now = tt.time()
        # Flush the batch if it's full or the time window has passed
        if len(self.batch) >= self.batch_size or (now - self.last_batch_time >= self.time_window):
            self._flush(now)

    def on_close(self):
        """Ensures any remaining data in the batch is printed on shutdown."""
        if self.batch:
            self._flush(tt.time())

    def _flush(self, now):
        """Prints the current batch of data and resets it."""
        print(f"\n[BatchObserver] Processing batch of {len(self.batch)} rows:")
        print("=" * 60)
        # Uncomment the following lines to print each row in the batch
        # for r in self.batch:
        #     print(r)
        self.batch = []
        self.last_batch_time = now
        print(f"Batch processed. Waiting for next batch...")
        print("=" * 60)


# 1) Read from MongoDB using Airbyte Connector
# The pw.io.airbyte.read function orchestrates running the specified
# Docker container and feeding it the configuration from the YAML file.

# Fixed call:
# - The docker_image is now passed as a named argument.
# - The config_path points to our simplified YAML file.
mongo_tbl = pw.io.airbyte.read(
    config_file_path="connections/mongodb_source.yaml",
    docker_image="airbyte/source-mongodb-v2:latest",
    streams=["tlg.book"],
    mode="streaming"
)

# 2) Write the output to our custom Python observer
# This will print the data in batches as it arrives from MongoDB.
pw.io.python.write(mongo_tbl, PrintBatchObserver(batch_size=10, time_window=5))

# 3) Launch the Pathway computation graph
print("Starting Pathway data pipeline...")
print("Listening for changes in MongoDB stream 'tlg.book'...")
pw.run()
