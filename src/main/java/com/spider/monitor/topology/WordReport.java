package com.spider.monitor.topology;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class WordReport extends BaseRichBolt {

	private static final long serialVersionUID = 886149197481637894L;
	private OutputCollector collector;
	private Map<String, AtomicInteger> counterMap;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.counterMap = new HashMap<String, AtomicInteger>();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		int count = input.getInteger(1);
		System.out.println("WordReport收到=============       " + word+":"+count);
	}

	@Override
	public void cleanup() {
		Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
		while (iter.hasNext()) {
			Entry<String, AtomicInteger> entry = iter.next();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
