Collate Objects from S3
=======================

1. Uses async http to have many concurrent S3 object get requests from a few python threads
2. Example tool `s3-to-tar` turns a list of urls read from `stdin` into a tar archive on `stdout` or on disk

Using WOfS yamls as a sample dataset I get following performance:

- Instance type `r4.xlarge`, 4 cores, 32Gb of memory
- Reading 100K documents completes in about 4 minutes when running 2 concurrent tasks
  - That's 240s, ~420 datasets per second per worker
  - 840 datasets per second per instance
  - Output is pumped into `gzip -3` then dumped to file

In comparison a simple fetch one object at a time using `boto3` is about 30
small objects per second per thread.

Processing a single chunk looks like this:

```bash
#!/bin/bash

chunk="$1"
time_file="${chunk}-time.txt"
tar_file="${chunk}.tgz"

echo "${chunk} -> ${time_file}, ${tar_file}"
exec /usr/bin/time -vv -o "${time_file}" s3-to-tar < "${chunk}" | gzip -3 > "${tar_file}"
```

Chunks were generated with this:

```bash
#!/bin/bash
split -d --lines 100000 ../urls.txt wofs-
```

Then finally:

```bash
#!/bin/bash
find . -name 'wofs-??' | sort | xargs -n 1 -P 2 ./process-chunk.sh
```

Downloading all 2.6 million WOfS meatadata documents from S3 took less than
hour, this is higher throughput than the yaml parser can parse.

Limitations
-----------

1. Assumes small documents
   - reads whole object into RAM
   - large internal queues
