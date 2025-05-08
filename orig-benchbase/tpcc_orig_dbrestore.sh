#!/bin/bash
set -e

result_dir=$1

##file name
change_config=Config
wh=$2
type_name=$3
output=""
orig_output="wh${wh}_orig_output"


## Perform some validation on input arguments, one example below
if [[ (-z "$1") || (-z "$2") || (-z "$3") ]]
then
        echo "Missing arguments, exiting.."
        echo "Usage : $0 arg1 arg2 arg3"
        exit 1
fi

if [ "$type_name" = "orig"  ]; then
	output=$orig_output
	for (( t=50; t<=120; t+=5 ))
	do
		java -cp .:lib/* $change_config $t >> prep_output
		sudo ssh -n node0 bash /users/Xueyuan/set_ssd_from_client.sh
		java -jar benchbase.jar -b tpcc -c config/mysql/sample_tpcc_config.xml --create=false --load=false --execute=true -d $result_dir/orig/wh${wh}/t${t} >> exec_output
	done || exit 1
else
	echo "Error happens in the passing-in parameter!"
	exit 1
fi

echo "Done!" >> $output
