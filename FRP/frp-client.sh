#!/bin/sh
pwd;
cp frpc.ini config.ini
frpc -c config.ini
