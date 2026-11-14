#!/usr/bin/env bash
set -eu
ACTION=${1:?start|stop}; K=${2:?number of VMs}

for i in $(seq 1 "$K"); do
  vm=small$i
  case $ACTION in
    start)
      Q=${3:?QNAME}; T=${4:-8}; P=${5:-1}
      ssh -n "$vm" "mkdir -p ~/logs; cd ~/c553-hw6 && source ~/ck.env || exit 1;
        nohup venv/bin/python3 src/worker.py -s $Q -t $T -p $P \
          >> ~/logs/worker-$Q.log 2>&1 < /dev/null &"
      echo "$vm: worker started (qname=$Q threads=$T prefetch=$P)" ;;
    stop)
      # SIGTERM: finish running tasks, then exit
      ssh -n "$vm" 'pkill -TERM -f "[s]rc/worker.py" || true;
                    while pgrep -f "[s]rc/worker.py" >/dev/null; do sleep 0.5; done'
      echo "$vm: worker stopped" ;;
    *) echo "usage: $0 start K QNAME [THREADS] [PREFETCH] | stop K"; exit 1 ;;
  esac
done
