# Modular footer query prototype

Status: completed

## Format

A complete modular-footer file retains the original `PAR1` header and every byte through the end
of the data and index region. It replaces the standard footer with a modular metadata blob and a
20-byte trailer:

```text
[unchanged file prefix][modular metadata]
[modular start: little-endian i64][root offset: little-endian i64][MFP1]
```

The root offset and every module offset are relative to the start of the modular metadata. This
keeps metadata-only blobs relocatable and allows the same encoded modules to be appended to a
complete data file.

## Reader

Footer detection remains internal to the metadata reader. `PAR1` files follow the standard path;
`MFP1` files load the modular root, schema, placement, file metadata, and row-group statistics and
adapt them to Hardwood's existing metadata records. The page reader then follows the original
column-chunk offsets and reads the unchanged data pages normally.

Metadata absent from the modular representation uses conservative defaults. In particular,
missing encoding statistics disable dictionary-based row-group pruning, and missing page-index
locations disable page-index pruning. Row-group min/max statistics retain their exactness flags,
so truncated bounds remain safe for predicate pruning.

## Scope

This is an experimental compatibility path for benchmarking the modular representation. It adds
no public API and does not change standard Parquet behavior.
