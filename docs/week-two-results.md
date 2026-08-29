Week Two - Message Broker and Database Persistence

Purpose

The second milestone extends the UDP/TCP transport layer into a complete asynchronous log pipeline:

UDP/TCP -> Collector -> RabbitMQ or Kafka -> Consumer -> MongoDB

The milestone requires raw payload preservation, provider selection through configuration, an independent consumer, durable persistence, real-infrastructure tests, recovery behavior, consumer scaling, batch insertion, and benchmark measurements for 1, 5, and 20 TCP clients.

Implementation Summary

Provider-independent collector

The UDP and TCP listeners produce the same LogRecord representation. The collector converts each record to a BrokerMessage and submits it through a provider-independent producer contract.

BROKER_PROVIDER selects either RabbitMQ or Kafka. No listener, consumer, or database code changes are required when switching providers.

Lossless message contract

Each broker message contains:

Schema version

Stable UUID message identifier

UTC receive timestamp

UDP or TCP protocol

Socket source IP and port

Readable raw-log text

Base64-encoded original bytes

Original payload size

The Base64 value is decoded and validated against both the declared byte length and readable text when a consumer reconstructs the message.

RabbitMQ behavior

Robust connection

Durable queue

Persistent messages

Publisher confirmations

Manual consumer acknowledgements

Prefetch limit of 500

Requeue after processing failure

Kafka behavior

Three topic partitions

acks=all

Idempotent producer

Consumer group coordination

Manual offset commits

Offset rewind after processing failure

Earliest-offset recovery for new groups

MongoDB behavior

Separate consumer-owned database connection

Majority and journaled write concern

Batch size of 500 by default

Unordered bulk upserts

messageId as document _id

Unique message-ID index

Descending receive-time index

BSON binary copy of the raw payload

Broker acknowledgement occurs only after MongoDB confirms the batch. Repeated deliveries match the existing _id and are counted as idempotent duplicates rather than inserted again.

Docker Compose Topology

RabbitMQ topology:

collector -> RabbitMQ broker -> consumer replicas -> MongoDB

Kafka topology:

Kafka volume initializer -> Kafka broker
collector -> Kafka broker -> consumer replicas -> MongoDB

The provider-specific Compose file is selected through BROKER_PROVIDER. Both stacks use health checks, named volumes, restart policies, and graceful stop periods. Kafka uses an initialization service to establish volume ownership before the broker starts.

Requirement Traceability

Requirement

Status

Evidence

UDP -> broker -> database

Complete

Real RabbitMQ and Kafka pipeline tests validate exact raw bytes and metadata

TCP -> broker -> database

Complete

Every sent TCP frame is found in MongoDB

Concurrent UDP and TCP

Complete

Mixed-transport integration test passes with both providers

1, 5, and 20 TCP clients

Complete

Integration and benchmark runs cover all three client counts

Consumer stopped

Complete

Broker backlog grows and drains after the consumer restarts

Broker stopped

Complete

Collector stays active, buffers records, reconnects, and publishes after restart

Collector restart

Complete

Broker and consumer continue independently; new records persist afterward

Consumer restart

Complete

Pending records persist after restart without duplicates

Real infrastructure

Complete

Tests use actual TCP, UDP, RabbitMQ/Kafka, consumers, and MongoDB in Docker

Batch insert

Complete

Consumer writes batches through unordered MongoDB bulk operations

Three consumer replicas

Complete

Scaling test verifies 100 unique messages are each persisted once

Provider configuration

Complete

BROKER_PROVIDER selects RabbitMQ or Kafka without source changes

25,000-record workload

Complete

All final runs persist 20,000 TCP and 5,000 UDP records

CPU and memory report

Complete

Docker resource samples are stored with every benchmark result

Integration Test Evidence

General suite

Ran 188 tests
OK (skipped=14)

The 14 skipped cases are opt-in Docker-backed suites. They are executed separately with their corresponding environment flags.

Broker pipeline suite

Both RabbitMQ and Kafka passed five real-infrastructure scenarios:

Lossless UDP persistence

Lossless TCP persistence

Simultaneous TCP and UDP

1, 5, and 20 concurrent TCP clients

Three consumers with duplicate protection

RabbitMQ: 5 tests passed
Kafka: 5 tests passed

Recovery suite

Both providers passed four service-interruption scenarios:

Messages wait while the consumer is stopped

Collector recovers after broker restart

Collector restart does not affect broker or consumer

Pending messages survive consumer restart

RabbitMQ: 4 tests passed
Kafka: 4 tests passed

Benchmark Method

Each final run uses the same workload so client-concurrency effects can be compared directly:

Setting

Value

TCP records

20,000

UDP records

5,000

Total records

25,000

TCP clients

1, 5, or 20

Consumer replicas

3

UDP pacing

250 records/second

UDP burst size

1

Consumer batch size

500

Resource interval

0.5 seconds requested

The script creates a unique queue or topic and consumer group for every run. This prevents earlier data, offsets, or backlog from affecting a measurement.

Measurement starts immediately before all sender workers are released. It stops when MongoDB contains the expected number of benchmark records. The result includes send time, broker/database drain time, end-to-end duration, logs per second, validation counts, and service-level resource samples.

Every record has a unique payload. Validation fails if it finds a missing payload, unexpected payload, duplicate payload, duplicate message ID, incorrect protocol count, or missing resource metrics.

The broker-reached count is 25,000 for every passing run because MongoDB has no collector-side write path: each of the 25,000 documents was received by a consumer from the selected broker after confirmed production.

Final Benchmark Results

Provider

Clients

Sent

Broker-delivered

MongoDB

Duration (s)

Logs/s

Avg CPU (%)

Avg memory (MiB)

RabbitMQ

1

25,000

25,000

25,000

20.181

1,238.786

773.649

793.019

RabbitMQ

5

25,000

25,000

25,000

20.238

1,235.280

729.134

621.504

RabbitMQ

20

25,000

25,000

25,000

21.111

1,184.200

776.598

615.145

Kafka

1

25,000

25,000

25,000

23.990

1,042.109

495.899

1,057.601

Kafka

5

25,000

25,000

25,000

24.296

1,028.966

548.237

1,019.961

Kafka

20

25,000

25,000

25,000

24.388

1,025.080

527.862

923.063

Avg CPU is the sum of the average Docker CPU percentages for the broker, collector, consumer replicas, and database. Values can exceed 100 percent because Docker reports CPU usage across logical cores.

Avg memory is the sum of the average memory use reported for the same services. Consumer measurements aggregate all three replicas.

Twenty-client Service Detail

Provider

Service

Avg CPU (%)

Peak CPU (%)

Avg memory (MiB)

Peak memory (MiB)

RabbitMQ

Broker

335.963

740.370

157.782

225.100

RabbitMQ

Collector

85.246

97.710

36.705

36.910

RabbitMQ

Consumers (3)

240.616

277.440

102.331

102.930

RabbitMQ

MongoDB

114.773

218.960

318.327

422.800

Kafka

Broker

284.933

435.150

500.333

678.100

Kafka

Collector

32.889

47.210

36.653

36.800

Kafka

Consumers (3)

127.016

146.870

101.994

102.290

Kafka

MongoDB

83.024

159.930

284.083

305.200

Performance Interpretation

RabbitMQ produced higher throughput in all three client scenarios:

Clients

RabbitMQ throughput advantage

1

18.9%

5

20.1%

20

15.5%

RabbitMQ used more aggregate CPU. Kafka used more aggregate memory, driven primarily by the Kafka broker process. In the 20-client result, RabbitMQ drained the broker and database stage in 1.087 seconds, while Kafka required 4.371 seconds.

Increasing the client count from 1 to 20 reduced RabbitMQ throughput by approximately 4.4 percent and Kafka throughput by approximately 1.6 percent. Both systems still persisted every record.

The results describe this specific implementation, Docker Desktop environment, consumer count, message size, persistence configuration, and paced UDP workload. They should not be interpreted as universal RabbitMQ or Kafka capacity limits.

UDP Pacing Diagnostics

UDP has no acknowledgement or retransmission mechanism. Initial burst-oriented runs demonstrated loss in the Docker Desktop forwarding path even though every TCP record persisted:

Provider

UDP rate

Burst

Persisted

Missing UDP

Outcome

RabbitMQ

2,000/s

100

24,338

662

Failed validation

Kafka

2,000/s

100

23,841

1,159

Failed validation

RabbitMQ

500/s

10

24,998

2

Failed validation

RabbitMQ

250/s

1

25,000

0

Passed

Kafka

250/s

1

25,000

0

Passed

The diagnostic files are preserved rather than discarded. They demonstrate why the final benchmark uses 250 UDP messages per second with a burst size of one. This pacing isolates the application pipeline from an unacknowledged host-to-container UDP burst limit.

Reproduction

Run the final matrix after Docker Desktop is ready:

python scripts\pipeline_benchmark.py --provider rabbitmq --tcp-clients 1
python scripts\pipeline_benchmark.py --provider rabbitmq --tcp-clients 5 --skip-build
python scripts\pipeline_benchmark.py --provider rabbitmq --tcp-clients 20 --skip-build
python scripts\pipeline_benchmark.py --provider kafka --tcp-clients 1 --skip-build
python scripts\pipeline_benchmark.py --provider kafka --tcp-clients 5 --skip-build
python scripts\pipeline_benchmark.py --provider kafka --tcp-clients 20 --skip-build

Each successful invocation prints Benchmark status: PASSED and writes a JSON result under benchmark-results.

Conclusion

The second-week milestone is complete. The same collector and consumer code operates with RabbitMQ and Kafka, every required interruption scenario recovers correctly, three consumers persist unique records without duplication, and all six final benchmark runs deliver 25,000 of 25,000 records to MongoDB.