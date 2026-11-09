# ADR 0007: The event log on Kafka

Status: accepted (phase 3)

## Decisions
- **The log is the system of record.** Engine state is a snapshot plus a replay of the log.
- **One topic per symbol (`mx.events.<symbol>`), one partition.** Ordering is total per symbol and recovery reads one partition. This is "partitioned by symbol" taken literally: the symbol is the partition. Many symbols mean many topics, which is acceptable here. Retention is infinite (`retention.ms=-1`), because truncating the log would destroy the state it defines.
- **One Kafka record per engine command**: an `EventBatch` holding every event that command produced (accept, trades, remainder cancel). A command is therefore atomic in the log. Recovery never sees half a command, and a snapshot or consumer never has to guess whether the remainder of an order rested. Each event inside still carries its own contiguous per-symbol sequence number, and each record carries a schema version that readers check.
- **Replay applies recorded outcomes; it does not re-run commands.** `Book.ApplyEvents` rebuilds state from events (an incoming order that still has quantity and was not cancelled rested). This keeps replay independent of the matcher's logic, and it is checked against the live book after every command in tests.
- **Asynchronous, batched, ordered producer** (idempotent, acks from all in-sync replicas). The engine goroutine never touches it: events flow engine goroutine, output ring, dispatcher, bounded local queue, Kafka.
- **Backpressure, not loss.** If Kafka is slow or down, batches accumulate in the bounded queue. At a high watermark of unacknowledged batches the gateway refuses new orders (`RESOURCE_EXHAUSTED`) before they reach the engine. The queue is sized above the watermark plus the in-flight limit so admission control, not queue overflow, is what pushes back. A permanent Kafka error stops admission and fails `/readyz` rather than dropping events.
- **Kafka client: franz-go.** It has an idempotent producer with ordering across retries, a callback-based asynchronous API with per-record acknowledgements (needed for the durable follower) and low overhead. `segmentio/kafka-go` was the alternative; its producer exposes less control over in-flight ordering and per-record offsets.

## The durability window (important)
The response to PlaceOrder is sent from the output buffer, **before** Kafka acknowledges the events. If the process is killed, the events the engine produced but Kafka had not yet acknowledged are lost, and clients may have seen acknowledgements for them. The window is small and observable (`mx_engine_last_sequence` minus `mx_eventlog_acked_sequence`; measured in BENCHMARKS.md), but it is not zero. After a crash the log is the truth: recovered state is exactly what the log holds, and the engine resumes after the last durable sequence, so there is no gap and no duplicate in the log. Closing the window would require delaying client acknowledgements until Kafka acknowledges, trading latency for durability; that is a deliberate non-goal for now.

## Not done here
The gateway's idempotency table (ADR 0006) is still in memory. After a restart a client cannot cancel an order placed before the crash by its `client_order_id`, and a retry of a pre-crash `client_order_id` is not recognised. Making idempotency durable (carrying the client id in the log and rebuilding the table on recovery) is a follow-up.
