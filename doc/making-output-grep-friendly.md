# Problem

 - The output is often prepended by general information or stats (as shown in Example 1)
 - Repeated headers: The header TYPE SIZE VOLUME PATH FINGERPRINT is currently printed before every single group (e.g. 3,500+ times).
 - Truncated fingerprints: Fingerprints currently truncate with trailing ... (2518a64e62652f13...), making copy-pasting or searching by hash slightly awkward.


## Example 1
kgorelov@klaptop:~/git/findex$ ./bin/findex compare --common 62FC6C44FC6C1497 8cb9bfb8-1b9e-490d-ae2e-aedb0d502701
Volume Comparison (--common (exist in both sets))
  Left Volumes:          62FC6C44FC6C1497
  Right Volumes:         8cb9bfb8-1b9e-490d-ae2e-aedb0d502701
  Type Filter:           all
  Total Source Content:  23510 entries (222.15 GB)
  Matching in Target:    4 entries (3 B)
  Absent from Target:    23506 entries (222.15 GB)

Common Entries (4):
TYPE       SIZE  VOLUME            PATH                                         FINGERPRINT
directory  -     62FC6C44FC6C1497  /Pictures/Import/Xiaomi-13T-Pro/Camera/Raw   bacb0480c27be7b7...
file       3 B   62FC6C44FC6C1497  /Pictures/gphotos-sync/gphotos.bad_ids.yaml  f5cd3bbe3413727d...
file       0 B   62FC6C44FC6C1497  /Pictures/gphotos-sync/gphotos.lock          9a25342b34f134d5...
file       0 B   62FC6C44FC6C1497  /Pictures/gphotos-sync/gphotos.trace         9a25342b34f134d5...


# Solution

 - Only output additional info and stats when the -v (--verbose) flag is passed.
 - Implement --columns option for all commands:
   The format will allow to specify any column ordering:
   findex compare --columns volume,path,size,type  --common 62FC6C44FC6C1497 8cb9bfb8-1b9e-490d-ae2e-aedb0d502701
 - Implement --format option: tsv,cvs,jsonl
 - Add group_id column
 - Do not add "..." to the truncated fingerprint
