## Submission

This update responds to CRAN's notice about the scheduled archival of `Boom`. `bsts` and
`BoomSpikeSlab`, which depend on `Boom`, move from Imports to Suggests and are used
conditionally, so EconCausal installs and loads without them.

## R CMD check results

0 errors | 0 warnings | 0 notes

`R CMD check --as-cran` was run on Fedora Linux with R 4.6.1 twice: with all suggested
packages installed, and in a library without `Boom`, `bsts` and `BoomSpikeSlab`
(`_R_CHECK_FORCE_SUGGESTS_=false`), which is the situation after the archival of `Boom`.

## Reverse dependencies

There are no reverse dependencies.
