#!/bin/bash

cd /home/monitor/work/data_stream_monitor/event_report

./lib/event_report_monitor_hourly.py  #> runtime_log_hourly 2>&1

