#!/bin/bash

# $1 is name
# $2 is topology
# $3 is algorithm
function run_topology() {
	iterations=10
	topology="$1"
	algorithm="$2"
	name=$(echo "$topology"_"$algorithm")

	storm jar target/storm-starter-0.10.0-SNAPSHOT-jar-with-dependencies.jar \
		  storm.starter.TopologyTester "$name" "$topology" "$algorithm" "$iterations"

	for i in `seq 1 45`; do
		storm list | grep INACTIVE | grep $1
		if [ "$?" -eq "0" ]; then
			echo "Topology $1 is INACTIVE"
			break
		fi
		sleep 60
	done
	echo "Killing Topology $1"
	storm kill "$1"
}

mvn clean compile assembly:single || exit

run_topology "wordcount" "trained"
run_topology "wordcount" "iterative"

run_topology "wordcount" "trained"
run_topology "wordcount" "iterative"

run_topology "wordcount" "trained"
run_topology "wordcount" "iterative"
