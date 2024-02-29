#!/bin/bash

# log
LOGFILE=bench.log
RESULTS=$1
DBNAME=$2

benchmark_dss() {
  print_log "- Running queries defined in TPC-H benchmark"

  mkdir -p $RESULTS
  
  for n in `seq 1 22`
  do
    q="queries/$n.js"

    if [ -f "$q" ]; then
      print_log "  running query $n"
      echo "RUNNING QUERY $n"
		  sudo mongosh $DBNAME --file $q > $RESULTS/result-$n 2> $RESULTS/error-$n
    fi;

  done;
}

print_log() {
  message=$1
  echo `date +"%Y-%m-%d %H:%M:%S"` "["`date +%s`"] : $message" >> $RESULTS/$LOGFILE;
}

rm -rf $RESULTS
mkdir -p $RESULTS

print_log "Running TPC-H benchmark"
benchmark_dss
print_log "Finished TPC-H benchmark"
