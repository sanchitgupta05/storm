#!/bin/bash

# $1 is topology
# $2 is algorithm
function run_topology() {
	iterations=10
	topology="$1"
	algorithm="$2"
	name=$(echo "$topology"_"$algorithm")

	while sleep 1; do
		storm jar target/storm-starter-0.10.0-SNAPSHOT-jar-with-dependencies.jar \
			  storm.starter.TopologyTester \
			  "$name" "$topology" "$algorithm" "$iterations"
		if [ "$?" -eq "0" ]; then
			break
		fi
	done

	for i in `seq 1 45`; do
		echo "[$i] Checking Topology $name"
		storm list | grep INACTIVE | grep "$name"
		if [ "$?" -eq "0" ]; then
			echo "Topology $name is INACTIVE"
			break
		fi
		sleep 60
	done

	echo "Killing Topology $name"
	storm kill "$name"
}

mvn clean compile assembly:single || exit

# run_topology "wordcount" "iterative"
# run_topology "wordcount" "trained"
# run_topology "wordcount" "roundrobin"

run_topology "custom0" "iterative"
run_topology "custom0" "trained"
run_topology "custom0" "roundrobin"
