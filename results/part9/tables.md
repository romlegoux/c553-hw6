E2 (8 VMs, base.en)
run | time (s) | speed | min busy VM (s) | max busy VM (s)
e2-utt-1x8 | 520 | 37.4 | 508 | 510
e2-utt-2x4 | 404 | 48.1 | 397 | 398
e2-utt-4x2 | 361 | 53.9 | 354 | 355
e2-utt-8x1 | 336 | 57.9 | 324 | 327
e2-chapter-8x1 | 562 | 34.6 | 202 | 353

E1 (8x1, base.en)
VMs | time (s) | speed | speedup | efficiency
1 | 1805 | 10.8 | 1.00 | 100%
2 | 898 | 21.6 | 2.01 | 100%
4 | 464 | 42.0 | 3.89 | 97%
8 | 336 | 57.9 | 5.37 | 67%

E3 (8 VMs, 8x1)
model | time (s) | speed | WER | mean job latency (s) | read/job (MB) | written/job (kB)
base.en | 336 | 57.9 | 4.53% | 328 | 4.22 | 7.0
tiny.en | 171 | 113.7 | 5.64% | 167 | 4.22 | 7.0
5 worst base.en: 1995-1836 20.4%, 4992-41797 10.8%, 121-123859 10.7%, 8463-294825 9.7%, 8555-284447 9.1%
5 worst tiny.en: 4992-41797 11.8%, 4992-41806 11.8%, 61-70970 11.7%, 8555-284449 11.5%, 8463-294825 10.9%

9d (kill -9 on ckw8)
2707 tasks ok, 87/87 jobs complete, merges per job: 1-1, time 337 s
