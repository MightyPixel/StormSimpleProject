package main.java.net.mightypixel.storm.math;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class MathTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("numberSpout", new NumberSpout());
		topologyBuilder.setBolt("numberBolt", new DoubleNumberBolt()).shuffleGrouping("numberSpout");

		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(1);

			StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, topologyBuilder.createTopology());
			Utils.sleep(3000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
