package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/kelseyhightower/envconfig"
)

const ConfigPrefix = "kdemo"

type Config struct {
	Region     string `default:"us-west-2"`
	StreamName string `default:"kinesistest"`
}

type Payload struct {
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}

func (c *Config) payloadGenerator(writeChan chan *Payload) {
	log.Println("payloadGenerator - starting...")
	ticker := time.NewTicker(time.Millisecond * 1000)
	for timestamp := range ticker.C {
		log.Printf("payloadGenerator - writeChan depth: %d - timestamp: %v", len(writeChan), timestamp)
		writeChan <- &Payload{
			Event:     "hi mom",
			Timestamp: timestamp.UTC().String(),
		}
	}
}

func (c *Config) streamWriter(client *kinesis.Kinesis, writeChan chan *Payload) {
	log.Println("streamWriter - starting...")
	for payload := range writeChan {
		marshaled, err := json.Marshal(payload)
		if err != nil {
			log.Printf("streamWriter - error marshaling payload '%+v': %s", payload, err)
		}
		input := &kinesis.PutRecordInput{
			Data:         marshaled,
			PartitionKey: &payload.Timestamp,
			StreamName:   &c.StreamName,
		}
		for {
			log.Printf("streamWriter - Kinesis input: %+v", input)
			output, err := client.PutRecord(input)
			if err != nil {
				log.Printf("streamWriter - error putting record to Kinesis: %s", err)
				log.Printf("streamWriter - sleeping 5 seconds before retrying")
				time.Sleep(5 * time.Second)
				continue
			}
			log.Printf("streamWriter - shard / seq: %s / %s", *output.ShardId, *output.SequenceNumber)
			break
		}
	}
}

func (c *Config) shardReader(client *kinesis.Kinesis, id int, shardId *string) error {
	log.Printf("shardReader[%d] - starting...", id)

	shardIteratorType := kinesis.ShardIteratorTypeTrimHorizon

	// Get initial shard iterator
	getShardIteratorInput := &kinesis.GetShardIteratorInput{
		ShardId:           shardId,
		ShardIteratorType: &shardIteratorType,
		StreamName:        &c.StreamName,
	}
	getShardIteratorOutput, err := client.GetShardIterator(getShardIteratorInput)
	if err != nil {
		log.Printf("shardReader[%d] - error getting shard iterator: %s", id, err)
		return err
	}
	iterator := getShardIteratorOutput.ShardIterator

	ticker := time.NewTicker(time.Millisecond * 2000)
	for range ticker.C {
		getRecordsInput := &kinesis.GetRecordsInput{
			ShardIterator: iterator,
		}
		log.Printf("shardReader[%d] - getting records...", id)
		output, err := client.GetRecords(getRecordsInput)
		if err != nil {
			log.Printf("shardReader[%d] - error getting records: %s", id, err)
		}
		if len(output.Records) > 0 {
			log.Printf("shardReader[%d] - records found: %d", id, len(output.Records))
			for _, rec := range output.Records {
				payload := &Payload{}
				err := json.Unmarshal(rec.Data, payload)
				if err != nil {
					log.Printf("shardReader[%d] - error unmarshalling record: %s", id, err)
					continue
				}
				log.Printf("shardReader[%d] - read payload from record: %+v", id, payload)
			}
		} else {
			// log.Printf("shardReader[%d] - no records found!", id)
		}
		if output.NextShardIterator == nil {
			log.Printf("shardReader[%d] - stream has closed!", id)
			break
		}
		iterator = output.NextShardIterator
	}
	log.Printf("shardReader[%d] - exiting...", id)
	return nil
}

// shardWatcher describes the Kinesis stream on a regular basis, creating new
// shardReader goroutines if a previously-unseen shard becomes available.
func (c *Config) shardWatcher(client *kinesis.Kinesis) {
	log.Println("shardWatcher - starting...")

	// shardId -> shardIsActive
	var knownShards sync.Map
	shardCount := 0

	ticker := time.NewTicker(time.Millisecond * 5000)
	for range ticker.C {
		log.Printf("shardWatcher - describing stream '%s'...", c.StreamName)
		describeStreamInput := &kinesis.DescribeStreamInput{
			StreamName: &c.StreamName,
		}
		stream, err := client.DescribeStream(describeStreamInput)
		if err != nil {
			log.Printf("shardWatcher - error describing stream '%s': %s", c.StreamName, err)
			log.Printf("shardWatcher - sleeping 5 seconds before retrying")
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("shardWatcher - shards found: %d", len(stream.StreamDescription.Shards))
		for _, shard := range stream.StreamDescription.Shards {
			if val, ok := knownShards.Load(*shard.ShardId); ok {
				log.Printf("shardWatcher - shard previously seen: %s (active: %t)", *shard.ShardId, val)
			} else {
				log.Printf("shardWatcher - new shard found: %s", *shard.ShardId)
				shardCount++
				knownShards.Store(*shard.ShardId, true)
				// Remove from map when goroutine exits to make sure we
				// recover from a goroutine that's exited too early
				go func(id int, shardId *string) {
					err := c.shardReader(client, id, shardId)
					if err == nil {
						// The reader exited OK after its shard was closed and
						// all records were read! We'll remember it (but mark
						// it inactive) so we don't spin up another reader.
						log.Printf("shardWatcher - shardReader %d exited - updating knownShards...", id)
						knownShards.Store(*shardId, false)
					} else {
						// Something went wrong; we should retry! Ditch the
						// record of this shard ID so another reader is run.
						log.Printf("shardWatcher - shardReader %d exited with error: %s", id, err)
						knownShards.Delete(*shardId)
					}

				}(shardCount, shard.ShardId)

			}

		}
	}
}

func main() {
	log.Println("main - starting...")
	var config Config
	err := envconfig.Process(ConfigPrefix, &config)
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Printf("main - config: %+v", config)
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(config.Region),
	}))
	client := kinesis.New(sess)

	writeChan := make(chan *Payload, 100)

	// TODO pass in context
	var wg sync.WaitGroup
	wg.Add(3)
	go config.payloadGenerator(writeChan)
	go config.shardWatcher(client)
	go config.streamWriter(client, writeChan)
	wg.Wait()
}
