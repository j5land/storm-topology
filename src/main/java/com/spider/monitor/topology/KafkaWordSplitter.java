package com.spider.monitor.topology;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import net.sf.json.JSONObject;

public class KafkaWordSplitter extends BaseRichBolt {

	private static final long serialVersionUID = 886149197481637894L;

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String line = "";
		if(input.getString(0) instanceof String){
			line = input.getString(0);
		}
		System.out.println("KafkaWordSplitter收到" + line);
		try {
			JSONObject dataJson = JSONObject.fromObject(line);
			JSONObject orderInfoJson = JSONObject.fromObject(dataJson.getString("data"));
			String word = orderInfoJson.getString("payment");
			collector.emit(input, new Values(word));
		} catch (Exception e) {
			e.printStackTrace();
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	

}
