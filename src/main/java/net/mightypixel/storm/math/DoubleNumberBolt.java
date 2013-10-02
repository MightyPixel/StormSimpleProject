package main.java.net.mightypixel.storm.math;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DoubleNumberBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		int oldNumber = input.getInteger(0);
		oldNumber++;
		System.out.println("The number from the bolt[" + this + "] is: " + oldNumber);
		collector.emit(input, new Values());
		
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}

}
