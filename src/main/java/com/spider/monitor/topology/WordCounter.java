package com.spider.monitor.topology;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCounter extends BaseRichBolt {

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
		String word = "test";
		 if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
		         && input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
		          System.out.println("################################WorldCount bolt: "+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date()));
		          for (Map.Entry<String, AtomicInteger> map : counterMap.entrySet()) {
			          this.collector.emit(new Values(map.getKey(), map.getValue().intValue()));
		          }
		 }else{
	    	    if(input.getString(0) instanceof String){
		  			word = input.getString(0);
		  		}
		  		System.out.println("WordCounter收到=============       " + word);
		  		AtomicInteger ai = this.counterMap.get(word);
		  		int count = 1;
		  		if (ai == null) {
		  			ai = new AtomicInteger(1);
		  			this.counterMap.put(word, ai);
		  		}else{
		  			count = ai.addAndGet(1);
		  			this.counterMap.put(word, new AtomicInteger(count));
		  		}
	      }
		 collector.ack(input);
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
		declarer.declare(new Fields("word", "count"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();  
		    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);  
		    return conf;  
	}

}
