## Run it

Make sure your container engine is running (Docker Desktop — or if you used the Podman that came with Astro, swap `docker` for `podman` below). Then from the `kafka` folder:

**1. Start the broker:**
```
docker compose up -d
```
First time pulls the image (a minute or two). Check it's up with `docker compose ps` — you want `kafka` running.

**2. Create the topic** (explicit, so it's deliberate for your demo):
```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic new_orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
(If you skip this, the topic auto-creates the moment the producer sends — but running it explicitly is nice to show.)

**3. Run the scripts** in two terminals in the `kafka` folder:
```
# Terminal 1
python consumer.py

# Terminal 2
python producer.py
```
Same as before: 100 orders stream out at 1/sec, the consumer prints only the `HIGH-VALUE` ones (> 50), and `high_value_orders.json` fills up in the folder.

**4. Stop the broker when done:**
```
docker compose down
```

## Seeing the stream (optional, good for the video)

You can watch raw messages arrive with Kafka's built-in console consumer:
```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic new_orders --from-beginning --bootstrap-server localhost:9092
```