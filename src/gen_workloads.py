#!/usr/bin/env python3
import os
import random
import sys


def write(path, lines):
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"{path}: {len(lines)} tasks")


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "workloads"
    os.makedirs(out, exist_ok=True)

    write(f"{out}/sleep0-100k.txt", ["sleep 0"] * 100_000)
    write(f"{out}/sleep0-10k.txt", ["sleep 0"] * 10_000)
    for w in (1, 2, 4, 8):
        threads = 8 * w
        write(f"{out}/sleep10ms-w{w}.txt", ["sleep 10"] * (1000 * threads))
        write(f"{out}/sleep1s-w{w}.txt", ["sleep 1000"] * (100 * threads))
        write(f"{out}/sleep10s-w{w}.txt", ["sleep 10000"] * (10 * threads))
    write(f"{out}/sleep1s-1600.txt", ["sleep 1000"] * 1600)

    mixed = ["sleep 100"] * 3800 + ["sleep 10000"] * 200
    random.Random(553).shuffle(mixed)
    write(f"{out}/mixed-4000.txt", mixed)


if __name__ == "__main__":
    main()
