# Problem
Current findex compare command has a very non-intuitive semantics.


## Solution

### compare command

findex compare [--left | -- right | --common] left_volumes right_volumes

Always compares two volumes set.
--left - display files/directories which exist only in the left volumes set
--right - display files/directories which exist only in the right volumes set
--common - display files/directories which exist in both, left and right volumes sets

### uniq command
findex uniq volumes_set
Display files which exist only in this volumes set. This commmand is a syntax sugar for finfex compare --left volumes_set all_other_volumes


### lookup
findex lookup <fingerprint>
Lookup the fingerprint in the database. Partial fingerprints can be used.

findex lookup <filepath>
Open the file or walk a directory by <filepath>, calculate the fingerprint and perform a database lookup.
