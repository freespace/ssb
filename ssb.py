#!/usr/bin/env python

import sys
import os
import os.path as op
from uuid import uuid4
from copy import deepcopy
from datetime import datetime
from socket import gethostname
from hashlib import sha256

import peewee as pw

import click
from tqdm import tqdm

STORAGE_DB = pw.SqliteDatabase(None)
STORAGE_DB_PREFIX='ssb-storage'

LOG_DB = pw.SqliteDatabase(None)
LOG_DB_PREFIX='ssb-log'

def is_same_size(apath, bpath):
  a_stat = os.stat(apath)
  b_stat = os.stat(bpath)
  return a_stat.st_size == b_stat.st_size

class StorageDBModel(pw.Model):
  class Meta:
    database = STORAGE_DB

class LogDBModel(pw.Model):
  class Meta:
    database = LOG_DB

class BackupLogEntry(LogDBModel):
  source_path = pw.CharField(max_length=512, index=True)
  dest_path = pw.CharField(max_length=512, index=True)
  size = pw.IntegerField()
  timestamp = pw.DateTimeField()
  sha256_hash = pw.CharField()
  storage_uuid = pw.UUIDField()

class BackupLog(LogDBModel):
  uuid = pw.UUIDField()
  version = pw.IntegerField()
  host = pw.CharField()
  timestamp = pw.DateTimeField()
  backup_set_uuid = pw.UUIDField(null=True)
  db_path = None

  def __str__(self):
    return f'BackupLog(uuid={self.uuid} host={self.host} ts={self.timestamp})'

  @classmethod
  def new(cls):
    db_id = uuid4()
    db_name = str(db_id)
    db_path = op.join('.', f'{LOG_DB_PREFIX}-{db_name}.sqlite')
    LOG_DB.init(db_path)
    LOG_DB.create_tables([BackupLog, BackupLogEntry])

    backup_log = BackupLog(uuid=db_id,
                           version=1,
                           host=gethostname(),
                           timestamp=datetime.utcnow())

    backup_log.save()
    backup_log.db_path = db_path
    print('Created new log', backup_log)
    return backup_log

  @classmethod
  def load(cls, db_path):
    LOG_DB.init(db_path)
    backup_log = BackupLog.get(id=1)
    backup_log.db_path = db_path

    print('Using existing log', backup_log)
    return backup_log

  def log(self, fpath, storage):
    pass

class BackupSet(StorageDBModel):
  backup_dirs = pw.CharField(max_length=1024)
  timestamp = pw.DateTimeField()
  uuid = pw.UUIDField()
  name = pw.CharField(null=True)
  comment = pw.TextField(null=True)
  host = pw.CharField()
  version = pw.IntegerField()
  sequence_number = pw.IntegerField()

class FileTransaction(StorageDBModel):
  source_path = pw.CharField(max_length=512, index=True)
  dest_path = pw.CharField(max_length=512, index=True)
  size = pw.IntegerField()
  timestamp = pw.DateTimeField()
  sha256_hash = pw.CharField()
  version = pw.IntegerField()

  backup_set = pw.ForeignKeyField(BackupSet, backref='files')

class Storage(StorageDBModel):
  uuid = pw.UUIDField()
  version = pw.IntegerField()
  root = pw.CharField(max_length=512)

  def __str__(self):
    return f'Storage(root={self.root})'

  @classmethod
  def init(cls, dirpath, reuse=True):
    """
    :param dirpath: path to initialise
    :param reuse: if False and a Storage DB already exists then an exception
                  will be thrown. Otherwise the existing Storage DB will be used.
    :return: Storage object stored in the DB
    """
    db_path = None
    create_new = True
    for ent in os.listdir(dirpath):
      if ent.startswith(STORAGE_DB_PREFIX):
        if not reuse:
          raise Exception(f'Storage already initialised: {ent}')
        else:
          db_path = op.join(dirpath, ent)
          create_new = False

    if db_path is None:
      # create a new UUID for this storage
      storage_id = uuid4()
      db_name = f'{STORAGE_DB_PREFIX}-{str(storage_id)}.sqlite'
      db_path = op.join(dirpath, db_name)
      print('Creating new Storage at', db_path)

    STORAGE_DB.init(db_path)

    if create_new:
      STORAGE_DB.create_tables([BackupSet, FileTransaction, Storage])
      storage = Storage(uuid=uuid4(),
                        version=1,
                        root=dirpath)
      storage.save()
    else:
      storage = Storage.get(id=1)
      storage.root = dirpath
      storage.save()

    return storage

  def backup_file(self, fpath):
    """
    Backsup the specified file.

    Current implementation only detects out-of-space condition
    when it runs into it. It really should try to pre-allocate
    the space first.

    :param fpath: path to file to backup
    :return: True if file backedup, False if there
             is no space left to backup the file
    """
    try:
      outofspace = False
      # make really sure it is an absolute path
      fpath = op.abspath(fpath)
      dst = op.join(self.root, gethostname(), fpath[1:])
      assert dst.startswith(self.root)
      
      done = False

      print(f'{fpath} -> {self}...', end='')

      if op.exists(dst) and is_same_size(fpath, dst):
        done = True
        print('exists')
      else: 
        dstdir = op.dirname(dst)
        os.makedirs(dstdir, exist_ok=True)

        ifh = os.open(fpath, os.O_RDONLY)
        ofh = os.open(dst, os.O_WRONLY | os.O_CREAT)

      while not done:
        buf = os.read(ifh, 16*1024*1024)
        if len(buf):
          byteswritten = 0
          while byteswritten < len(buf):
            byteswritten += os.write(ofh, buf[byteswritten:])
        else:
          done = True
          print('done')
    except OSError as ex:
      if ex.errno == 28:
        # this means we are out of space so we need to go
        # to the next storage
        outofspace = True
        print('out of space')
      else:
        raise ex
    except Exception as ex:
      raise ex
    finally:
      try:
        os.close(ifh)
      except:
        pass

      try:
        os.close(ofh)
      except:
        pass

      if outofspace:
        # remove the partial file
        os.unlink(dst)
      else:
        assert is_same_size(fpath, dst)

      return done

@click.group()
def cli():
  pass

@cli.command()
@click.argument('storage_dir',
                type=click.Path(file_okay=True, exists=True, writable=True),
                required=True)
def storage_init(storage_dir):
  Storage.init(storage_dir)

@cli.command()
@click.option('-b', '--backup', 'backup_dirs', type=click.Path(exists=True), required=True,
              multiple=True,
              help='Directory or files to backup. Can be specified multiple times.')
@click.option('-s', '--storage', 'storages', type=click.Path(exists=True, file_okay=False, writable=True),
              required=True, multiple=True,
              help='Storage to backup into. Can be specified multiple times.')
@click.option('-r', '--resume-using', 'resume_log', type=click.Path(exists=True, dir_okay=False),
              help='Resume backup using log')
def backup(backup_dirs, storages, resume_log):
  if resume_log:
    backup_log = BackupLog.load(resume_log)
  else:
    backup_log = BackupLog.new()

  if backup_log.host != gethostname():
    st = click.confirm(f'Log host and current host differ ({backup_log.host} != {gethostname()}). Continue?')
    if not st:
      return

  def next_storage(existing_backup_set=None):
    if len(storages) == 0:
      return None, None

    storage = Storage.init(storages.pop(0))
    if existing_backup_set:
      new_bk_set = deepcopy(existing_backup_set)
      new_bk_set.id = None
      new_bk_set.sequence_number += 1
      new_bk_set.save()
    else:
      new_bk_set = None

    print('Current Storage:', storage)
    return storage, new_bk_set

  storages = list(storages)
  cur_storage, _ = next_storage()

  if backup_log.backup_set_uuid:
    cur_bk_set = BackupSet.get(uuid=backup_log.backup_set_uuid)
  else:
    cur_bk_set = BackupSet(uuid=uuid4(),
                           backup_dirs='\0'.join(backup_dirs),
                           timestamp=datetime.utcnow(),
                           host=gethostname(),
                           version=1,
                           sequence_number=0)
    cur_bk_set.save()

  for bakdir in backup_dirs:
    for (root, dirnames, filenames) in os.walk(bakdir):
      for fn in filenames:
        fpath = op.join(root, fn)
        fpath_abs = op.abspath(fpath)

        if not BackupLogEntry.select().filter(source_path=fpath_abs).exists():
          try:
            done = False
            while not done:
              done = cur_storage.backup_file(fpath_abs)
              if not done:
                cur_storage, cur_bk_set = next_storage(cur_bk_set)
                if cur_storage is None:
                  # we have no more storage left, bail!
                  backup_log.save()
                  print(f'No more Storage left. Attach additional Storage and resume using '
                        f'\n\t{sys.argv[0]} --resume-using {backup_log.db_path} ...')
                  return
          except Exception as ex:
            raise ex
          else:
            backup_log.log(fpath_abs, cur_storage)

  print('Backup complete')

if __name__ == '__main__':
  cli()
