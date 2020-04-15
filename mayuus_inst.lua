#!/usr/bin/env tarantool

box.cfg {
    listen = 3301,
    background = true,
    log = '/var/log/tarantool/mayuus_app.log',
    pid_file = '/var/run/tarantool/mayuus_app.pid',
}

require 'mayuus_app'.init()
