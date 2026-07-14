# consumer

Praetor's **event consumer** service. It binds the durable JetStream consumers on
the [`eventbus`](https://github.com/praetordev/eventbus) and persists what flows
through them:

- **job events** → written to the database (the ack-after-commit contract means a
  DB outage is replayed from the stream, not lost), and
- **log chunks** → indexed into `job_output_chunks`,

dispatching notifications on lifecycle events along the way.

It is a leaf deployable: nothing imports it in production. It depends only on the
shared `praetordev/*` libraries (`eventbus`, `events`, `db`, `notify`, `crypto`,
`metrics`, `env`, `plog`). The reusable core lives under [`core/`](core/) so
integration tests in other repos can drive an in-process writer.

## Run

```
DATABASE_URL=postgres://... NATS_URL=nats://... go run .
```

## Build the image

```
docker build -t praetor-consumer:latest .
```

The image name is stable (`praetor-consumer`) so the Helm chart and k3d/kind load
step are unaffected by this repo split.

## Tests

Unit tests run standalone. The DB-backed integration tests are gated on
`TEST_DATABASE_URL` and skip without it.

```
TEST_DATABASE_URL=postgres://... go test ./...
```
