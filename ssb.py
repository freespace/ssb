#!/usr/bin/env python

import sys
import os
try:
  import readline; assert readline
except:
  pass
import platform
import os.path as op
from uuid import uuid4
from copy import deepcopy
from datetime import datetime
from socket import gethostname
from hashlib import sha256

import peewee as pw

import click

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
  sha256hash = pw.CharField()
  storage_uuid = pw.UUIDField()

  def __str__(self):
    return f'storage:{self.storage_uuid}\t{self.source_path}'

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

  def log(self, file_transaction, storage):
    entry = BackupLogEntry(source_path=file_transaction.source_path,
                           dest_path=file_transaction.dest_path,
                           size=file_transaction.size,
                           timestamp=file_transaction.timestamp,
                           sha256hash=file_transaction.sha256hash,
                           storage_uuid=storage.uuid)
    entry.save()


class BackupSet(StorageDBModel):
  backup_dirs = pw.CharField(max_length=1024)
  timestamp = pw.DateTimeField()
  uuid = pw.UUIDField()
  name = pw.CharField(null=True)
  comment = pw.TextField(null=True)
  host = pw.CharField()
  version = pw.IntegerField()
  sequence_number = pw.IntegerField()
  is_final = pw.BooleanField()

class FileTransaction(StorageDBModel):
  source_path = pw.CharField(max_length=512, index=True)
  dest_path = pw.CharField(max_length=512, index=True)
  size = pw.IntegerField()
  timestamp = pw.DateTimeField()
  sha256hash = pw.CharField()
  version = pw.IntegerField()

  backup_set = pw.ForeignKeyField(BackupSet, backref='files')

  def __str__(self):
    hp = f'{self.backup_set.host}:{self.source_path}'
    return f'{hp:64}\t{self.size}\t{self.sha256hash}'

class Storage(StorageDBModel):
  uuid = pw.UUIDField()
  version = pw.IntegerField()
  root = pw.CharField(max_length=512)

  def __str__(self):
    return f'Storage(root={self.root})'

  def disconnect(self):
    STORAGE_DB.close()

  def print_info(self):
    print('Storage Info')
    print('============')
    print(f'root: {self.root}')
    print(f'uuid: {self.uuid}')

    fcount = FileTransaction.select().count()
    print(f'Files: {fcount}')

    bkidset = set()
    finalcount = 0
    for bkset in BackupSet.select():
      bkidset.add(bkset.uuid)
      if bkset.is_final:
        finalcount += 1

    bksetcount = len(bkidset)
    print(f'Backup Sets: {bksetcount}')
    print(f'  Finals: {finalcount}')

    print('')

  def record_transaction(self, src, dst, sha256hash, bk_set):
    src_size = os.stat(src).st_size
    ft = FileTransaction(source_path=src,
                         dest_path=dst,
                         size=src_size,
                         timestamp=datetime.utcnow(),
                         sha256hash=sha256hash,
                         version=1,
                         backup_set=bk_set)
    ft.save()
    return ft

  @classmethod
  def init(cls, dirpath, reuse=True, exists=False):
    """
    :param dirpath: path to initialise
    :param reuse: if False and a Storage DB already exists then an exception
                  will be thrown. Otherwise the existing Storage DB will be used.
    :param exists: requires the Storage DB to exist already. Will not create
                   a new Storage DB
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
      if exists:
        raise Exception(f'Storage not found at {dirpath}')
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

  def backup_file(self, fpath, bk_set):
    """
    Backsup the specified file.

    Current implementation only detects out-of-space condition
    when it runs into it. It really should try to pre-allocate
    the space first.

    :param fpath: path to file to backup
    :param bk_set: the BackupSet the file belongs to
    :return: (FileTransaction, outofspace) ->
    FileTransaction created if backup successful, None otherwise.
    outofspace, True if backup failed due to lack of space in Storage,
    False otherwise.
    """
    try:
      outofspace = False
      tf = None

      # make really sure it is an absolute path
      fpath = op.abspath(fpath)
      
      sysname = platform.system()
      if sysname == 'Linux':
        # remove leading /
        dstsuffix = fpath[1:]
      if sysname == 'Windows':
        # remove : from c:\
        dstsuffix = fpath[0] + fpath[2:]
       
      suffix = op.join(gethostname(), dstsuffix)
      suffix = suffix.replace(':', '')
      dst = op.join(self.root, suffix)
      
      assert dst.startswith(self.root)
 
      done = False

      print(f'{fpath} -> {self}...', end='')

      if op.exists(dst) and is_same_size(fpath, dst):
        print('exists')

        done = True
        tf = FileTransaction.get(dest_path=dst)
        assert tf, f'File {dst} exists but is not recorded in a FileTransaction'
        tf = self.record_transaction(fpath, dst, tf.sha256hash, bk_set)
      else:
        # we do this to pre-allocate space for the transaction in the DB
        # otherwise it is possible to copy the file but run out of space
        # for it in the database so the backup goes unrecorded
        tf = self.record_transaction(fpath, dst, '0'*64, bk_set)

        dstdir = op.dirname(dst)
        os.makedirs(dstdir, exist_ok=True)

        ifh = os.open(fpath, os.O_RDONLY)
        ofh = os.open(dst, os.O_WRONLY | os.O_CREAT)

        m = sha256()
        while not done:
          buf = os.read(ifh, 4*1024)
          m.update(buf)
          if len(buf):
            byteswritten = 0
            while byteswritten < len(buf):
              byteswritten += os.write(ofh, buf[byteswritten:])
          else:
            done = True
        if done:
          print('done')
          tf.sha256hash = m.hexdigest()
          tf.save()
    except OSError as ex:
      if ex.errno == 28:
        # this means we are out of space so we need to go
        # to the next storage
        outofspace = True
        print('out of space (file)')
      else:
        print('Error', type(ex), ex)
        raise ex
    except pw.OperationalError as ex:
      if str(ex) == 'database or disk is full':
        outofspace = True
        print('out of space (database)')
      else:
        print('Error', type(ex), ex)
        raise ex
    except Exception as ex:
      print('Error', ex)
      raise ex
    else:
      assert is_same_size(fpath, dst)
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
        FileTransaction.delete_instance(tf)
        tf = None

      return tf, outofspace

@click.group()
def cli():
  pass

@cli.command()
@click.argument('storage_dir',
                type=click.Path(file_okay=False, exists=True),
                required=True)
def storage_init(storage_dir):
  Storage.init(storage_dir)

@cli.command()
@click.argument('storage_dir',
                nargs=-1,
                type=click.Path(file_okay=False, exists=True),
                required=True)
def storage_ls(storage_dir):
  for sdir in storage_dir:
    Storage.init(sdir, reuse=True, exists=True)
    for tf in FileTransaction.select():
      print(tf)

@cli.command()
@click.argument('storage_dir',
                nargs=-1,
                type=click.Path(file_okay=False, exists=True),
                required=True)
def storage_info(storage_dir):
  for sdir in storage_dir:
    s = Storage.init(sdir, reuse=True, exists=True)
    s.print_info()

@cli.command()
@click.argument('log_db',
                type=click.Path(dir_okay=False, exists=True),
                required=True)
def log_ls(log_db):
  BackupLog.load(log_db)
  for ent in BackupLogEntry.select():
    print(ent)
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

  def get_next_storage(existing_backup_set=None):
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

  # we must do this before do anything else with BackupSet
  # bc Storage.init() is called by next_storage() which
  # opens the backing DB
  cur_storage, _ = get_next_storage()

  if backup_log.backup_set_uuid:
    cur_bk_set = BackupSet.get(uuid=backup_log.backup_set_uuid)
    print('Resuming Backupset', cur_bk_set)
  else:
    cur_bk_set = BackupSet(uuid=uuid4(),
                           backup_dirs='\0'.join(backup_dirs),
                           timestamp=datetime.utcnow(),
                           host=gethostname(),
                           version=1,
                           sequence_number=0,
                           is_final=False)
    cur_bk_set.save()
    backup_log.backup_set_uuid = cur_bk_set.uuid
    backup_log.save()

    print('Created new Backupset', cur_bk_set)

  for bakdir in backup_dirs:
    for (root, dirnames, filenames) in os.walk(bakdir):
      for fn in filenames:
        fpath = op.join(root, fn)
        fpath_abs = op.abspath(fpath)

        # if the file exists in the backup log then skip it
        if BackupLogEntry.select().filter(source_path=fpath_abs).exists():
          print(f'Skip {fpath_abs}')
        else:
          try:
            done = False
            while not done:
              file_transaction, outofspace = cur_storage.backup_file(fpath_abs, cur_bk_set)
              if outofspace:
                # disconnect so the storage can be removed from the host
                cur_storage.disconnect()
                cur_storage = None
                while cur_storage is None:
                  next_storage, next_bk_set = get_next_storage(cur_bk_set)
                  if next_storage:
                    cur_storage = next_storage
                    cur_bk_set = next_bk_set
                  else:
                    new_storage = click.prompt('No more storage left. Enter path '
                                               'to new Storage or "STOP" to exit program')
                    if new_storage == 'STOP':
                      print(f'Attach additional Storage and resume using '
                            f'\n\t{sys.argv[0]} --resume-using {backup_log.db_path} ...'
                            f'\nOr run again and see if we can fit smaller files around existing files')
                      return
                    elif op.exists(new_storage):
                      storages.append(new_storage)
                    else:
                      print('Invalid path', new_storage)

              else:
                assert file_transaction, 'Not out of space but also no file transaction recorded'
                backup_log.log(file_transaction, cur_storage)
                done = True
          except Exception as ex:
            raise ex
  cur_bk_set.is_final = True
  cur_bk_set.save()
  print('Backup complete')

if __name__ == '__main__':
  cli()
