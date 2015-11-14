#!/bin/sh
#$1 nr Iterations
#$2 nr Vms

rm -r ~/Desktop/BoodieRepo3/Cassandra-VMS/experiment_logs/*
for i in $(seq 1 $1);
do
	echo "starting script ..."
	pkill -f CassandraDaemon
	rm -r ~/Desktop/Cassandra-VMS/logs/*
	rm -r ~/Desktop/Cassandra-VMS/data/*
	~/Desktop/Cassandra-VMS/bin/cassandra 
	sleep 120
	java ~/Desktop/Cassandra-VMS/src/java/ViewManager/Vm_Prop_Restart_Conf
        java ~/Desktop/Cassandra-VMS/src/java/ViewManager/BootVMS
	read -p "Press any key..."
	mkdir ~/Desktop/Cassandra-VMS/experiment_logs/i
	mv ~/Desktop/Cassandra-VMS/experiment_logs/* ~/Desktop/BoodieRepo3/Cassandra-VMS/experiment_logs/i
done
