# Kinesis API demo

## What is this?

A toy application to learn about the [Kinesis](https://aws.amazon.com/kinesis/data-streams/) API and provide functional examples for when I inevitably forget how any of this works.

This application:

  * writes a simple payload to a Kinesis stream
  * manages a dynamic pool of shard reader goroutines, keeping track of what shards have been previously seen.

## Usage

```bash
export KDEMO_REGION="us-west-2"
export KDEMO_STREAM_NAME="kinesistest"
./kinesisdemo
```

Uses [``dep``](https://github.com/golang/dep) for dependency management.

## Resources

Docs for aws-sdk Kinesis package: [https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis](https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis)
