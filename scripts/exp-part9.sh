#!/usr/bin/env bash
# Part 9 (9d, 9e) on the node, in tmux: scripts/exp-part9.sh STEP [T X]
#   smoke        2 chapters on 1 VM (2x4): checks the whole chain             (~3 min)
#   e2           utterance tasks, 8 VMs, shapes -t x -x = 1x8 2x4 4x2 8x1      (~40 min)
#   chapter T X  chapter tasks, 8 VMs, best shape                              (~10 min)
#   e1 T X       utterance tasks on 1, 2 and 4 VMs, best shape (8 VMs = e2)    (~1.5 h)
#   e3 T X       tiny.en on 8 VMs, best shape (base.en = e2)                   (~5 min)
#   kill T X     9d: kill -9 the worker on ckw8 60 s in, 8 VMs                 (~10 min)
# One run each (about 10 min or more). Results: results/part9/<step>/
set -u
source "$(dirname "${BASH_SOURCE[0]}")/exp-lib.sh"
JOBS=workloads/jobs-test-clean.txt
step=${1:?step}; T=${2:-}; X=${3:-}
DIR0=$RES/part9

session() {   # Q W T X DIR
  PROV_ARGS="-t $3 -x $4" session_start "$1" "$2" 1 "$5"
}

case $step in
  smoke)
    on_tiny "head -2 $JOBS > workloads/jobs-smoke.txt"
    session "p9-smoke" 1 2 4 "$DIR0/smoke"
    one_run "smoke" -j workloads/jobs-smoke.txt -g utt -m base.en
    session_end ;;
  e2)
    for shape in "1 8" "2 4" "4 2" "8 1"; do
      set -- $shape
      session "p9-${1}x${2}" 8 "$1" "$2" "$DIR0/e2"
      one_run "e2-utt-${1}x${2}" -j "$JOBS" -g utt -m base.en
      session_end
    done ;;
  chapter)
    session "p9-chap" 8 "$T" "$X" "$DIR0/e2"
    one_run "e2-chapter-${T}x${X}" -j "$JOBS" -g chapter -m base.en
    session_end ;;
  e1)
    for w in 1 2 4; do
      session "p9-w$w" "$w" "$T" "$X" "$DIR0/e1"
      one_run "e1-w$w-${T}x${X}" -j "$JOBS" -g utt -m base.en
      session_end
    done ;;
  e3)
    session "p9-tiny" 8 "$T" "$X" "$DIR0/e3"
    one_run "e3-tiny-${T}x${X}" -j "$JOBS" -g utt -m tiny.en
    session_end ;;
  kill)
    session "p9-kill" 8 "$T" "$X" "$DIR0/kill"
    KILL_AFTER=60 one_run "9d-kill-${T}x${X}" -j "$JOBS" -g utt -m base.en
    session_end ;;
  *) echo "unknown step: $step"; exit 1 ;;
esac
log "done"
