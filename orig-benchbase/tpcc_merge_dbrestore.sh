#!/bin/bash
set -e

##directory: local-nonuniform/cross-nonuniform/cross-uniform/local-uniform
result_dir=$1

##file name
change_config=Config
wh=$2
merge=$3
output=""
merged_output="wh${wh}_merge${merge}_output"
orig_output="wh${wh}_orig_output"
grpc_output="wh${wh}_grpc_output"
type_name=$4
thread=$5

## Permform some validation on input arguments, one example below
if [[ (-z "$1") || (-z "$2") || (-z "$3")]]
then
        echo "Missing arguments, exiting.."
        echo "Usage : $0 arg1 arg2 arg3"
        exit 1
fi

if [ "$type_name" = "grpc"  ]; then
	output=$grpc_output
	for (( t=1; t<=120; t+=1 ))
	do
		java -cp .:lib/* $change_config $t >> prep_output
		sudo ssh -n node0 bash /users/Xueyuan/set_from_client.sh
        # java -jar benchbase.jar -b tpcc -c config/mysql/sample_tpcc_config.xml --create=false --load=false --execute=true -d $result_dir/wh${wh}/orig/t${t} >> exec_output
		java -jar benchbase.jar -b tpcc -c config/mysql/sample_tpcc_config.xml --create=false --load=false --execute=true -d $result_dir/wh${wh}/orig/t${t} >> exec_output
	done || exit 1
elif [ "$type_name" = "merged"  ]; then
	output=$merged_output
	for (( t=100; t<=500; t+=100 ))
	do
		java -cp .:lib/* $change_config $t >> prep_output
		sudo ssh -n node0 bash /users/Xueyuan/set_ssd_from_client.sh
		sudo ssh -n node1 bash /users/Xueyuan/set_grpc_from_client.sh $wh $merge $thread &
		sleep 5
		java -jar benchbase.jar -b spreegrpc -c config/mysql/sample_spreegrpc_config.xml --create=false --load=false --execute=true -d $result_dir/wh${wh}/merge${merge}/t${t} >> exec_output
	done || exit 1
else
	echo "Error happens in the passing-in parameter!"
	exit 1
fi

echo "Done!" >> $output
