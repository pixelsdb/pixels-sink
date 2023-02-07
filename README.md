# pixels-sink
The data sink for Pixels. It ingests data into Pixels in a batch or streaming manner.

Currently, it only provides a command-line tool for macro-benchmark evaluations (e.g., TPC-H).
We can use it to load data from csv files into Pixels, copy the data, compact the small files,
and run the benchmark queries.

It was previously named `pixels-load` as its earliest functionality was to load data for the evaluations.
Streaming and real-time loading will be supported in the future.