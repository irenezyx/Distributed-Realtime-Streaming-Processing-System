# CS 425 MP4 - Crane

This project builds Crane, a real-time stream processing system that is faster than Spark Streaming on small data sets. It supports bolts filter and transform. 

MP1 was used to debug this project. MP2 provides distributed group membership service. MP3 provides distributed file system(SDFS).

## Usage

### Start SDFS
Refer to [MP3 README.md](https://gitlab-beta.engr.illinois.edu/jyuan18/cs_425_mp3/blob/master/README.md).

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
MP2Log onto that machine. Run the file  [*command.py*](command.py) with leave instruction *"p"* and specifies the value to be *"m"*:
```
python command.py {\"p\":\"m\"}
```
