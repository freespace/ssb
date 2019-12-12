SSB: Steve's Stupid Backup
==========================

Motivation
----------

The goal of Steve's Stupid Backup is to backup files across multiple
drives in a way that requires no special filesystems or drivers. This
means:

- It works with FAT32 filesystems which is universally readable even
  though it has filename restrictions and no symlink support which would
  have supported a form of de-duplication
- It is designed to backup a single large directory tree to multiple
  harddrives. This is intended to allow backing up of NAS setups where
  the aggregate storage cannot be realised in a single disk. It does
  this in manner such that:
  - you don't need special software to reconstruct the backup set
  - you don't need all the drives to begin recovery
  - you don't need to have all drives connected at the time of backup

Design
------

### Overview

SSB uses `BackupSet` to identify files that were marked for backup at
the same time. `BackupSet`s can span `Storage`s. When they do the
`BackupSet` is chained such that each `Storage` instance contains
a  `BackupSet` object is recorded in `Storage` instances with the same
`uuid` and incrementing `sequence_number`. This allows us to identify,
given a random bunch of disks, which ones are required to reconstruct
a particular `BackupSet`.

During backup SSB writes a `BackupLog` that allows it to resume
an interrupted backup and to identify which `Storage`s was used for
which `BackupSet`.

### BackupSet

A `BackupSet` is a set of files marked for backup at the same time.
A `BackupSet` consists of:

- `backup_dirs`: directories to backup, abspath, null separated
- `timestamp`: time of backup, UTC time
- `uuid`: unique identifier
- `name`: user assigned name
- `comment`: user assigned comment
- `host`: the host computer performing the backup
- `version`: `BackupSet` version
- `sequence_number`: 0, 1, ..., identifies where this `BackupSet` is in
   the overall sequence
- `files`: list of `FileTransaction` objects
- `is_final`: 1 if this is the last `BackupSet` in the sequence,
   0 otherwise.

### FileTransaction

A `FileTransaction` records the backup of a file. It has the following
properties:

- `source_path`: abspath of the source on the host, e.g. `/data/`
- `dest_path`: abspath of the backup copy on the host, e.g.
  `/media/steve/SANDISK16GB`
- `timestamp`: backup time in UTC
- `size`: file size in bytes
- `sha256_hash`: sha256 hash of the file
- `version`: version of `FileTransaction`

### Storage
A `Storage` is a folder where files in BackupSets are stored. This is
typically a mounted folder, e.g. `/media/steve/SANDISK16G`. Each
`Storage` is identified by a `ssb-storage.sqlite` file which
contains ORM of `Storage` objects consisting of:

- `uuid`: unique identifier of the `Storage`
- `backup_sets`: list of `BackupSet` objects to be found in this
  `Storage`
- `version`: `Storage` version

In `Storage` files are stored using their absolute path under a folder
named after the hostname of the computer running SSB. This provides
a rudimentry form of namespacing.

### BackupLog
- `uuid`: uuid of the `BackupLog`
- `version`: version of the `BackupLog`
- `host`: host computer that was running SSB
- `timestamp`: UTC timestamp of when the log was first created
- `backup_sets`: list of (`BackupSet`, `Storage.uuid`) objects that have
   been committed to `Storage`

Use cases
---------

## Backup of 15TB NAS onto 3x6TB Drives

1. SSB creates `BackupSet` and starts writing to the first `Storage` (A)
1. SSB runs out of space on A, writes a `BackupSet` object into A
   with `segment_number` of 0 and `is_final` of 0
1. SSB moves to the next `Storage` given via commandline, or prompts the
   user for a new drive, or offer the exit and resume later
1. SSB creates a new `BackupSet` object with the same uuid
1. SSB continues copying onto `Storage` B
1. SSB exhausts B, writes the `BackupSet` into B with `sequence_number`
   of 1 and `is_final` of 0
1. SSB continues to `Storage` C, another `BackupSet` with the same uuid
   as the first is created to record `Files`
1. SSB finishes backing up onto C, writes `BackupSet` into C with
   `sequence_number` of 2 and `is_final` of 1.
1. Backup complete!

Limitations
===========

So many...

Inefficient Use of Storage
--------------------------

SSB doesn't try to optimise storage utilisation by distributing large
and small files. e.g. If there is a big file that cannot be stored in
storage A SSB will automatically move to the storage B even though the
next file may fit on A. This is because SSB doesn't use OS specific
means of working out how much storage is on A, which means it has to
workout of the file fits on known Storages by attempting to write it out
which is very expensive time wise. Additionally SSB doesn't enumerate
all files and their sizes prior to backup. Therefore SSB has no way of
knowing that there is a subsequent file that will fit on A.

This can be worked around by running the backup once, and when it runs
out of space run it a few more times with `--resume-using`. Each time
SSB will try to find space for new files and maybe allow a backup
to complete even though the first run doesn't.

Duplicate Files
---------------

Related to the above the same file may not be backed up to the same
Storage across backup runs, e.g. if you specify Storage in a different
order. This could be fixed by aggregating all known Storages when
backing up.
