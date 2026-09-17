## Resubmission

This is a resubmission of 1.0.3, which failed the incoming pre-tests on
r-devel-windows-x86_64 and r-devel-linux-x86_64-debian-gcc. The failing test set two BLAS and
two OpenMP threads and expected to read them back, which does not hold with the reference BLAS
or when `RhpcBLASctl` is built without OpenMP. The thread handling is now tested with an
in-memory controller on every platform, and the tests against the real `RhpcBLASctl` are
skipped when a thread count set through it does not read back. Both failures were reproduced
locally before the fix and no longer occur. The version is increased to 1.0.4.

## Submission

This update responds to CRAN's notice about the scheduled archival of `Boom`. `bsts` and
`BoomSpikeSlab`, which depend on `Boom`, move from Imports to Suggests and are used
conditionally, so EconCausal installs and loads without them.

## R CMD check results

0 errors | 0 warnings | 0 notes

`R CMD check --as-cran` was run on Fedora Linux with R 4.6.1 four times: with all suggested
packages installed and OpenBLAS; in a library without `Boom`, `bsts` and `BoomSpikeSlab`
(`_R_CHECK_FORCE_SUGGESTS_=false`), which is the situation after the archival of `Boom`; with
the reference BLAS and `RhpcBLASctl` built without OpenMP, the configuration of the failed
pre-tests; and in a library without `RhpcBLASctl`.

## Reverse dependencies

There are no reverse dependencies.
