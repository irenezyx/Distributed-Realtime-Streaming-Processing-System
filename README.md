# Distributed Realtime Streaming Processing System

This project builds Crane, a real-time stream processing system whose structure is adapted from Storme and runs faster than Spark Streaming on small data sets. It supports fundamental functions like bolts filter and transform. 

This project includes a distributed system debugger, distributed group membership service and a distributed file system like HDFS.

## Usage

### Start SDFS
Refer to [SDFS README.md](SDFS_README.md).

### Start Standby Nimbus
Log onto the machine for it. Here we assign VM1 for standby nimbus. Run the file [*standby_nimbus.py*](standby_nimbus.py)
```
python standby_nimbus.py
```

### Start Nimbus
Log onto the machine for it (we assign VM2 here), and run the file [*nimbus.py*](nimbus.py):
```
python nimbus.py
```

### Start All Workers
Log onto other machines, run the file [*Supervisor.py*](Supervisor.py):
```
python Supervisor.py
```

### Submit an Application
First write the topology as example of word count with filter as in [*topology_specifier.py*](topology_specifier.py). Then write application as example in [*Client.py*](Client.py). Finally, run command in this format:
```
python Client.py Input_File Output_File
```
Without arguments Input_File and Output_File, default settings will be used, which are "wordcount.txt" and "frequency".

The output file will be saved in SDFS.

### Check Results of Application
As the output file has been stored in SDFS, we can run [*sdfs_interface.py*](sdfs_interface.py) with a "get" command. For example, if we want to save the results to file "fre" at local:
```
python sdfs_interface.py get frequency fre
```

### Print a Machine's Membership List
Log onto that machine. Run the file  [*command.py*](command.py) with leave instruction *"p"* and specifies the value to be *"m"*:
```
python command.py {\"p\":\"m\"}
```
