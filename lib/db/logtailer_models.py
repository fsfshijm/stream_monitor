# Create your models here.
# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#     * Rearrange models' order
#     * Make sure each model has one field with primary_key=True
# Feel free to rename the models, but don't rename db_table values or field names.
#
# Also note: You'll have to insert the output of 'django-admin.py sqlcustom [appname]'
# into your database.
from __future__ import unicode_literals

from django.db import models

class DbLogtailer(models.Model):
    id = models.IntegerField(primary_key=True)
    create_time = models.IntegerField()
    service_id = models.CharField(max_length=128L)
    user = models.CharField(max_length=64L)
    hostname = models.CharField(max_length=128L)
    port = models.IntegerField()
    pid = models.IntegerField()
    start_time = models.IntegerField()
    total_bytes_read = models.BigIntegerField()
    total_messages_send = models.BigIntegerField()
    total_threads = models.IntegerField()
    error_threads_num = models.IntegerField()
    snap_time = models.IntegerField()
    last_update_time = models.IntegerField()
    class Meta:
        db_table = 'logtailer'

class DbLogtailerThread(models.Model):
    id = models.IntegerField(primary_key=True)
    logtailer_id = models.IntegerField()
    status = models.CharField(max_length=32L)
    start_time = models.IntegerField()
    stop_time = models.IntegerField()
    thread_name = models.CharField(max_length=64L)
    stream_id = models.IntegerField()
    stream_name = models.CharField(max_length=64L)
    stream_reader = models.CharField(max_length=64L)
    stream_sender = models.CharField(max_length=64L)
    bytes_read = models.BigIntegerField()
    message_sent = models.BigIntegerField()
    reader_topic_name = models.CharField(max_length=64L)
    reader_group = models.CharField(max_length=64L)
    files_processed = models.IntegerField()
    current_file = models.CharField(max_length=256L)
    current_offset = models.BigIntegerField()
    read_error_count = models.IntegerField()
    sender_topic_name = models.CharField(max_length=64L)
    stream_script = models.CharField(max_length=256L)
    inline_class = models.CharField(max_length=256L)
    thrift_server_port = models.CharField(max_length=1024L)
    send_error_count = models.IntegerField()
    error_message = models.CharField(max_length=1024L)
    warning_message = models.CharField(max_length=1024L)
    last_update_time = models.IntegerField()
    class Meta:
        db_table = 'logtailer_thread'

class DbLogtailerThreadChangelog(models.Model):
    id = models.IntegerField(primary_key=True)
    logtailer_thread_id = models.IntegerField()
    logtailer_id = models.IntegerField()
    status = models.CharField(max_length=32L)
    start_time = models.IntegerField()
    stop_time = models.IntegerField()
    thread_name = models.CharField(max_length=64L)
    stream_id = models.IntegerField()
    stream_name = models.CharField(max_length=64L)
    stream_reader = models.CharField(max_length=64L)
    stream_sender = models.CharField(max_length=64L)
    bytes_read = models.BigIntegerField()
    message_sent = models.BigIntegerField()
    reader_topic_name = models.CharField(max_length=64L)
    reader_group = models.CharField(max_length=64L)
    files_processed = models.IntegerField()
    current_file = models.CharField(max_length=256L)
    current_offset = models.BigIntegerField()
    read_error_count = models.IntegerField()
    sender_topic_name = models.CharField(max_length=64L)
    stream_script = models.CharField(max_length=256L)
    inline_class = models.CharField(max_length=256L)
    thrift_server_port = models.CharField(max_length=1024L)
    send_error_count = models.IntegerField()
    error_message = models.CharField(max_length=1024L)
    warning_message = models.CharField(max_length=1024L)
    last_update_time = models.IntegerField()
    alarm_time = models.IntegerField()
    class Meta:
        db_table = 'logtailer_thread_changelog'

class DbLogtailerThreadStat(models.Model):
    id = models.IntegerField(primary_key=True)
    thread_id = models.IntegerField()
    snap_time = models.IntegerField()
    bytes_read = models.IntegerField()
    message_sent = models.IntegerField()
    read_error_count = models.IntegerField()
    send_error_count = models.IntegerField()
    class Meta:
        db_table = 'logtailer_thread_stat'

class DbLogtailerThreadStat5Min(models.Model):
    id = models.IntegerField(primary_key=True)
    thread_id = models.IntegerField()
    snap_time = models.IntegerField()
    bytes_read = models.BigIntegerField()
    message_sent = models.IntegerField()
    read_error_count = models.IntegerField()
    send_error_count = models.IntegerField()
    class Meta:
        db_table = 'logtailer_thread_stat_5min'

class DbLogtailerThreadStatHourly(models.Model):
    id = models.IntegerField(primary_key=True)
    thread_id = models.IntegerField()
    snap_time = models.IntegerField()
    bytes_read = models.BigIntegerField()
    message_sent = models.IntegerField()
    read_error_count = models.IntegerField()
    send_error_count = models.IntegerField()
    class Meta:
        db_table = 'logtailer_thread_stat_hourly'

class DbStream(models.Model):
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=64L)
    create_time = models.IntegerField()
    deleted = models.IntegerField()
    class Meta:
        db_table = 'stream'

class DbStreamUser(models.Model):
    id = models.IntegerField(primary_key=True)
    stream_id = models.IntegerField()
    username = models.CharField(max_length=128L)
    rel_type = models.CharField(max_length=9L)
    class Meta:
        db_table = 'stream_user'

