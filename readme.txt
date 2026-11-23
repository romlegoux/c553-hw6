
================================================================
CS553 Fall 2026 - HW6 - CloudKon clone on Chameleon
Romain Legoux (A20713663), Chameleon user: rlegoux
================================================================
 
1. DIRECTORY STRUCTURE
   src/         client.py, worker.py, provisioner.py, common.py (shared),
                mgmt.py, setup.py, healthcheck.py, gen_workloads.py,
                check-counts.py, analyze.py, fix-headers.py
                Part 9: jobs.py, transcribe.py, s3util.py, stage-data.py,
                examples.py, analyze-part9.py
   scripts/     testbed (build-*.sh, setup-ssh.sh, sync.sh, venv-vms.sh,
                bootstrap.sh), workers (workers.sh), worker image
                (build-image.sh, rebuild-image.sh, worker-*.sh),
                experiments (exp-lib.sh, exp-static.sh, exp-elastic.sh,
                exp-part9.sh), Part 9 data (stage-data.sh),
                infos.sh
   config/      ck-worker.service (systemd unit), var_env.txt (env template)
   workloads/   elastic.txt, jobs-test-clean.txt (others are generated)
   tests/       pytest tests of common.py
   results/     raw results: tests/ (Parts 2-5), static/<step>/ (7a-7d, Q6),
                elastic/ (7e), analysis/ (tables and plots),
                part9/ (e1 e2 e3 kill e3-again, tables.md, 9e1-speedup.png)
   sysinfo/     evidence C1-C3
 
2. REQUIREMENTS
   Chameleon compute_cascadelake_r (CHI@UC), Ubuntu 24.04, LXD 5.21,
   Docker 29, RabbitMQ 4.3, Cassandra 5.0.9, SeaweedFS 4.48, Python 3.12,
   pika 1.4.4, cassandra-driver 3.30.1 (+ pandas, matplotlib for analysis).
 
3. BUILDING THE TESTBED (on the node)
   Lease <LEASE NAME>, one compute_cascadelake_r node at CHI@UC, then:
       git clone git@github.com:romlegoux/c553-hw6.git && cd c553-hw6
       scripts/build-lxd.sh        # then log out and back in (group lxd)
       scripts/build-vm.sh         # tiny (4/8G), svc (8/32G), small1-8 (8/16G)
       scripts/setup-ssh.sh        # passwordless SSH from tiny
   (scripts/bootstrap.sh runs sections 3 and 4 in one go.)
 
4. DEPLOYING THE SERVICES (on svc)
       scripts/build-services.sh
   Starts RabbitMQ, Cassandra and SeaweedFS in Docker on svc, creates
   the Cassandra role ck, disables the default role, and copies the
   generated password to ~/ck_password on the node. Then:
       cp config/var_env.txt ~/ck.env    # put svc's IP and the password
       chmod 600 ~/ck.env                # never commit it
       scripts/sync.sh tiny small1 ... small8
 
5. BUILDING THE CODE (one command)
       scripts/build-venv.sh             # venv + requirements.txt
   On the node, and on the VMs with: scripts/venv-vms.sh tiny small1 ...
 
6. CONFIGURATION
   Load with "source ~/ck.env": CK_AMQP_URL, CK_CASSANDRA_HOSTS,
   CK_CASSANDRA_USER, CK_CASSANDRA_PASSWORD, CK_S3_ENDPOINT,
   CK_S3_ACCESS_KEY, CK_S3_SECRET_KEY.
   Optional: CK_NODE (node name in headers), CK_COMMIT,
   CK_DURABLE=0 (setup.py only: classic queues, for Q6).
 
7. SETUP AND HEALTHCHECK (on tiny)
       python3 src/healthcheck.py        # RabbitMQ, Cassandra, SeaweedFS: OK
       python3 src/setup.py demo         # queues demo.tasks / demo.results + tables
 
8. RUNNING THE FRAMEWORK (on tiny)
       python3 src/gen_workloads.py                         # workloads/*.txt
       python3 src/client.py -s LOCAL -t 8 -w workloads/sleep0-10k.txt   # local
       scripts/workers.sh start 2 demo 8 1                  # workers on small1-2
       python3 src/client.py -s demo -w workloads/sleep0-10k.txt -o demo.csv
       scripts/workers.sh stop 2
   Client options: -d P (duplicates), -S FILE (schedule: START DURATION
   RATE COMMAND per line), -o CSV.
   Elastic workers (on the node):
       scripts/build-image.sh            # image ck-worker from small1
       venv/bin/python3 src/provisioner.py -s demo --min 0 --max 8 --idle 30 \
           | tee -i prov.log             # Ctrl-C deletes its VMs
 
9. REPRODUCING THE EXPERIMENTS
       python3 src/gen_workloads.py && scripts/sync.sh tiny
       scripts/exp-static.sh smoke       # 3 min test
       scripts/exp-static.sh             # 7a-7d, ~3 h
       scripts/exp-static.sh q6          # Q6, ~10 min
       scripts/exp-elastic.sh            # 7e, ~70 min
   Steps of exp-static.sh: local (7a local), w1 w2 w4 w8 (7a, 7b; w8 also
   7c prefetch 1 and 7d), p10 and p100 (7c). Each run is done 3 times
   (once if longer than 10 min: 7a on 1 VM). Results go to
   results/static/<step>/ and results/elastic/.
   Part 9 (whisper.cpp v1.9.5 is built into the image by rebuild-image.sh):
       scripts/stage-data.sh             # test-clean + models -> SeaweedFS
       scripts/rebuild-image.sh          # image ck-worker with whisper.cpp
       scripts/exp-part9.sh e2           # E2: 1x8 2x4 4x2 8x1
       scripts/exp-part9.sh chapter 8 1  # E2: one task per chapter
       scripts/exp-part9.sh e1 8 1       # E1: 1, 2, 4 VMs (8 VMs = E2 8x1)
       scripts/exp-part9.sh e3 8 1       # E3: tiny.en
       scripts/exp-part9.sh kill 8 1     # 9d: kill -9 on ckw8 after 60 s
       scripts/exp-part9.sh examples MODEL JOB...   # one error per chapter
   Results go to results/part9/<step>/.
 
10. ANALYSIS
       venv/bin/python3 src/analyze.py
   Writes results/analysis/tables.md and the 7 plots of 7a-7e.
       venv/bin/python3 src/analyze-part9.py
   Writes results/part9/tables.md and 9e1-speedup.png.
 
11. EVIDENCE AND PROVENANCE
   Each raw file starts with:
     # vm=<VM> node=<node> utc=<time> commit=<git commit>
   fix-headers.py moved this line to the top of 120 files where driver
   warnings came first (nothing else changed).
   sysinfo/: node.txt, lease.png (C1), vms.txt (C2), services.txt,
   rabbitmq-ui.png (C3).
 
