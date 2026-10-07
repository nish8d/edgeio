# Running the edge agent on a device

The agent reads the host it runs on and publishes a `device.health` v1 report every 300 s to Kafka over Tailscale. If Kafka can't be reached, readings are spooled on disk for up to 7 days and sent oldest first when it comes back.

**Requirements on the device:**
- Linux with cgroup v2 (e.g. Ubuntu 22.04+)
- Docker
- Tailscale
- A user in the `docker` group. No sudo needed.

## 1. Start the stack with the Tailscale listener (server)

```sh
make up-tailnet        # Kafka also listens on <server tailscale ip>:9094
```

## 2. Build and ship the image (server → device)

```sh
make agent-image
docker save edgeio/agent:dev | gzip | ssh USER@DEVICE 'gunzip | docker load'
```

## 3. Configure (device)

Copy `deploy/agent/agent.env.example` to `~/edgeio-agent.env` on the device. Then set:
- `KAFKA_BOOTSTRAP` to the server's Tailscale IP with port `:9094`;
- `AGENT_SERVICE_RENAMES` for the container that is the edge streamer.

## 4. Run (device)

```sh
docker run -d --name edgeio-agent --restart=always \
  --network host --pid host --uts host \
  --log-opt max-size=10m --log-opt max-file=3 \
  -v /:/host:ro \
  -v /sys/fs/cgroup:/host-cgroup:ro \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v edgeio-agent-spool:/spool \
  --env-file ~/edgeio-agent.env \
  edgeio/agent:dev
docker logs -f edgeio-agent      # one JSON "tick" line per reading
```

The device appears on the dashboard under its Tailscale IP within one interval.

**Security note:** Docker socket access is root-equivalent, even when mounted read-only. The agent only issues GET requests to the Docker API.

## Upgrade / remove

```sh
docker rm -f edgeio-agent        # the spool volume is kept; re-run step 4 with the new image
docker volume rm edgeio-agent-spool   # only to discard unsent readings
```
