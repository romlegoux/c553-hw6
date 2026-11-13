#!/bin/bash
set -euo pipefail

VM=svc
PW_FILE_NODE="$HOME/ck_password"
lxc exec "$VM" -- bash -s <<'EOF'
set -euo pipefail
cd /root

if ! command -v docker >/dev/null; then
    apt-get update -q
    DEBIAN_FRONTEND=noninteractive apt-get install -y -q docker.io
fi

if [ ! -s ck_password ]; then
    openssl rand -hex 16 > ck_password
fi
chmod 600 ck_password
PW=$(cat ck_password)

exists() { docker container inspect "$1" >/dev/null 2>&1; }


if ! exists rabbitmq; then
    docker run -d --name rabbitmq --restart unless-stopped \
        -p 5672:5672 -p 15672:15672 -v rmq:/var/lib/rabbitmq \
        -e RABBITMQ_DEFAULT_USER=ck -e RABBITMQ_DEFAULT_PASS="$PW" \
        rabbitmq:4.3-management
fi

if [ ! -f cassandra.yaml ]; then
    docker run --rm cassandra:5.0 cat /etc/cassandra/cassandra.yaml > cassandra.yaml
    sed -i 's/^authenticator:.*/authenticator: PasswordAuthenticator/' cassandra.yaml
fi
if ! exists cassandra; then
    docker run -d --name cassandra --restart unless-stopped -p 9042:9042 \
        -v cass:/var/lib/cassandra \
        -v /root/cassandra.yaml:/etc/cassandra/cassandra.yaml \
        -e MAX_HEAP_SIZE=4G cassandra:5.0
fi

if ! exists seaweedfs; then
    docker run -d --name seaweedfs --restart unless-stopped \
        -p 8333:8333 -p 12535:12535 -v sw:/data \
        -e AWS_ACCESS_KEY_ID=ck -e AWS_SECRET_ACCESS_KEY="$PW" \
        chrislusf/seaweedfs:4.48
fi

cql() { docker exec cassandra cqlsh -u "$1" -p "$2" -e "$3"; }
PING="SELECT now() FROM system.local"

for i in $(seq 1 60); do
    if cql ck "$PW" "$PING" >/dev/null 2>&1 \
       || cql cassandra cassandra "$PING" >/dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "Cassandra does not respond" >&2
        exit 1
    fi
    sleep 5
done


if ! cql ck "$PW" "$PING" >/dev/null 2>&1; then
    cql cassandra cassandra \
        "CREATE ROLE ck WITH PASSWORD = '$PW' AND LOGIN = true AND SUPERUSER = true"
fi
cql ck "$PW" "ALTER ROLE cassandra WITH SUPERUSER = false AND LOGIN = false"

docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
EOF

lxc file pull "$VM/root/ck_password" "$PW_FILE_NODE"
chmod 600 "$PW_FILE_NODE"
echo "Services déployés. Mot de passe copié dans $PW_FILE_NODE"
