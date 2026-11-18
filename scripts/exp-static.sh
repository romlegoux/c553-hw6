#!/usr/bin/env bash
# Part 7a-7d and Q6, on the node in tmux: scripts/exp-static.sh [STEP...]
#   steps: smoke local w1 w2 w4 w8 p10 p100 q6 (default: local w1 w2 w4 w8 p10 p100)
set -u
source "$(dirname "${BASH_SOURCE[0]}")/exp-lib.sh"
WL=workloads

for step in ${*:-local w1 w2 w4 w8 p10 p100}; do
  case $step in
    smoke)
      session_start e-smoke 1 1 "$RES/static/smoke"
      MPSTAT=1 one_run smoke-r1 -w "$WL/sleep0-10k.txt"
      session_end ;;

    local)
      DIR=$RES/static/local; mkdir -p "$DIR"; Q=LOCAL
      for t in 1 2 4 8; do
        for r in 1 2 3; do
          on_tiny "mkdir -p ~/runs && venv/bin/python3 src/client.py -s LOCAL -t $t \
            -w $WL/sleep0-100k.txt -o ~/runs/7a-local-t$t-r$r.csv" > "$DIR/7a-local-t$t-r$r.out" 2>&1
          on_tiny "cat ~/runs/7a-local-t$t-r$r.csv" > "$DIR/7a-local-t$t-r$r.csv"
          log "7a-local-t$t-r$r: $(grep -E '^(time|throughput)' "$DIR/7a-local-t$t-r$r.out" | tr -s ' ' | tr '\n' ' ')"
        done
      done ;;

    w1|w2|w4|w8)
      W=${step#w}
      session_start "e-w$W" "$W" 1 "$RES/static/$step"
      MPSTAT=1 runs3 "7a-w$W"      -w "$WL/sleep0-100k.txt"
      runs3 "7b-10ms-w$W"          -w "$WL/sleep10ms-w$W.txt"
      runs3 "7b-1s-w$W"            -w "$WL/sleep1s-w$W.txt"
      runs3 "7b-10s-w$W"           -w "$WL/sleep10s-w$W.txt"
      if [ "$W" = 8 ]; then
        MPSTAT=1 runs3 "7c-mixed-p1" -w "$WL/mixed-4000.txt"
        runs3 "7d-i-dup"           -w "$WL/sleep0-10k.txt" -d 0.1
        runs3 "7d-ii-nofail"       -w "$WL/sleep1s-1600.txt"
        KILL_AFTER=10 runs3 "7d-ii-kill" -w "$WL/sleep1s-1600.txt"
      fi
      session_end ;;

    p10)
      session_start e-p10 8 10 "$RES/static/p10"
      MPSTAT=1 runs3 7c-sleep0-p10 -w "$WL/sleep0-100k.txt"
      session_end ;;

    p100)
      session_start e-p100 8 100 "$RES/static/p100"
      MPSTAT=1 runs3 7c-sleep0-p100 -w "$WL/sleep0-100k.txt"
      MPSTAT=1 runs3 7c-mixed-p100  -w "$WL/mixed-4000.txt"
      session_end ;;

    q6)
      SETUP_ENV="CK_DURABLE=0" session_start q6-fast 8 1 "$RES/static/q6"
      MPSTAT=1 runs3 q6-fast-w8 -w "$WL/sleep0-100k.txt"
      session_end ;;

    *) echo "unknown step: $step"; exit 1 ;;
  esac
done
log "all done"
