function run_topology() {
	iterations=10
	topology="$1"
	algorithm="$2"
	name=$(echo "local_$topology_$algorithm")

	mvn clean compile exec:java -Dstorm.topology=storm.starter.TopologyTester \
		-Dexec.args="$name $topology $algorithm $iterations 1"
}

# run_topology "custom0" "roundrobin"
# run_topology "custom0" "iterative"

# run_topology "custom0" "trained"
run_topology "custom0" "random"
