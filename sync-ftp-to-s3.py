import math
import os

from boto.s3.connection import S3Connection
import io
import paramiko
import stat
import time


chunk_size = 12428800
ftp_priv_key_filename = '/path/to/private/key'  # this script assume priv use auth
ftp_username = 'username'
ftp_host = 'myhost'
ftp_port = 22
ftp_dir = '/folder/on/ftp/server'
s3_id = 's3_id'
s3_key = 's3_key'
bucket_name = 'bucket_name'


s3_conn = S3Connection(s3_id, s3_key)
bucket = s3_conn.get_bucket(bucket_name)


pkey = paramiko.RSAKey.from_private_key_file(ftp_priv_key_filename)
transport = paramiko.Transport((ftp_host, ftp_port))
transport.connect(username=ftp_username, pkey=pkey)
ftp_conn = paramiko.SFTPClient.from_transport(transport)


def move_file(filepath):
    key_id = filepath.replace(ftp_dir, '').lstrip('/')
    key = bucket.get_key(key_id)
    ftp_fi = ftp_conn.file(filepath, 'r')
    source_size = ftp_fi._get_size()
    if key is not None:
        # check if we need to replace, check sizes
        if source_size == key.size:
            print('%s already uploaded' % key_id)
            ftp_fi.close()
            return

    chunk_count = int(math.ceil(source_size / float(chunk_size)))
    mp = bucket.initiate_multipart_upload(key_id)

    print('%s uploading size: %imb, %i chunks' % (
        key_id, math.ceil(source_size/1024/1024), chunk_count))
    for i in range(chunk_count):
        start = time.time()
        chunk = ftp_fi.read(chunk_size)
        end = time.time()
        seconds = end - start
        print('%s read chunk from ftp (%i/%i) %ikbs' % (
            key_id, i + 1, chunk_count,
            math.ceil((chunk_size / 1024) / seconds)))

        fp = io.BytesIO(chunk)
        start = time.time()
        mp.upload_part_from_file(fp, part_num=i + 1)
        end = time.time()
        seconds = end - start
        print('%s upload chunk to s3 (%i/%i) %ikbs' % (
            key_id, i + 1, chunk_count,
            math.ceil((chunk_size / 1024) / seconds)))

    mp.complete_upload()
    ftp_fi.close()


def move_dir(directory):
    ftp_conn.chdir(directory)
    for filename in ftp_conn.listdir():
        filepath = os.path.join(directory, filename)
        if stat.S_ISDIR(ftp_conn.stat(filepath).st_mode):
            move_dir(filepath)
        else:
            move_file(filepath)

move_dir(ftp_dir)