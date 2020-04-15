# mayuus_mcshw
Mayuulin's Homework

# Dependencies
* Tarantool 2.2
* Tarantool http rock

# How to use
1) Install Tarantool
2) cd / && tarantoolctl rocks install http
3) Edit mayuus_inst.lua (optional)
4) Edit server_settings parameters in mayuus_app.lua
5) Run sudo cp mayuus_app.lua /usr/share/tarantool/ (or check your app dir)
6) Run sudo cp mayuus_inst.lua /etc/tarantool/instances.enabled/mayuus_app.lua (or check your instances dir)
7) Run sudo tarantoolctl start mayuus_app
