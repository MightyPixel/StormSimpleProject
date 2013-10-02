package main.java.net.mightypixel.storm.math;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class NumberSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;

	SpoutOutputCollector collector;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(500);
		Random rand = new Random();
		int number = Math.abs(rand.nextInt()%10);

		System.out.println("The number from the spout[" + this + "] is: " + number);
		collector.emit(new Values(number));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}

}
