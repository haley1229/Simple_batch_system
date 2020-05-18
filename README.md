# Simple_batch_system
A simple distributed system that can do the job similar to map reduce
## Environment
Go 1.13
## Usage

### To run the machine as a introducer: 
(Default introducer is machine 5 and portNumber is 1234)
`go run main.go -m introducer`

### To run the machine as a server:
(Default portNumber is 1234)
`go run main.go -m -server -port=[portnumber]`

After starting the machine, type in `leave\n` to leave the membership, `join\n` to join the membership, `print\n` to print the list and neighbors, and `ID\n` to print the machine ID. 

## Simple Distributed File System
### To put local file to SDFS:
`put [local FileName] [SDFS FileName]`

### To put files in a folder to the SDFS with common name:
`putlist [folder] [common name in the files]`
All the files in the folder containing the common name will be put to the SDFS, with the same name.

### To get file from SDFS:
`get [SDFS FileName] [local FileName]`

### To delete file from SDFS:
`delete [SDFS FileName]`

### To list all machines that store certain file:
`ls [SDFS FileName]`

### To list the files stored on one machine:
`store`

## MapleJuice Framework

### Maple 
The maple stage can be run with the following command:

`maple [maple_exe] [num_maples] [sdfs_intermediate_filename_prefix] [sdfs_src_directory]`, 

The parameter `maple_exe` is an executable that takes as input one file and outputs a series of (key, value) pairs. 
The user need to write the MapFunc in one go file, and compile this file using the plugin mode in Golang to generate the executable file:

`go build -buildmode=plugin -o mapexample.so mapexample.go`.

`sdfs_intermediate_filename_prefix` specifies the generated file prefix, and `sdfs_src_directory` specifies the folder containing the input files. 

### Juice
The juice stage can be run with the following command:

`juice [juice_exe] [num_juices] [sdfs_intermediate_filename_prefix] [sdfs_dest_filename] [delete_input]`, 

The parameter `juice_exe` is an executable that runs the juice function that is similar to `maple_exe`. 
`sdfs_intermediate_filename_prefix` specifies the input file prefix, and `sdfs_src_directory` specifies the folder containing the output files.
`delete_input` can be 0 (will not delete the intermediate files) or 1 (will delete the intermediate files after the experiment).
