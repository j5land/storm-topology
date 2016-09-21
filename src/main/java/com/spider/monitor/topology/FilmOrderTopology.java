package com.spider.monitor.topology;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class FilmOrderTopology {

	public static void main(String[] args) throws Exception {

		String zks = "192.168.3.203:2181,192.168.3.205:2181,192.168.3.207:2181";
		String topic = "paymentCount";
		String zkRoot = "/storm"; // default zookeeper root configuration for storm
		String id = "paymentCount";
		BrokerHosts brokerHosts = new ZkHosts(zks, "/brokers");
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConf.zkServers = Arrays.asList(new String[] { "192.168.3.203", "192.168.3.205", "192.168.3.207" });
		spoutConf.zkPort = 2181;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
		builder.setBolt("word-splitter", new KafkaWordSplitter(), 1).shuffleGrouping("kafka-reader");
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));
		builder.setBolt("word-report", new WordReport()).globalGrouping("word-counter");

		Config conf = new Config();
		conf.setDebug(true);
		// Nimbus host name passed from command line
		String name = FilmOrderTopology.class.getSimpleName();
		if (args != null && args.length > 0) {
			//conf.put(Config.NIMBUS_SEEDS, FilmOrderTopology.class.getName());
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			System.out.println("=======================================================启动成功！！！");
			Thread.sleep(2000000);
			cluster.killTopology(name);
			cluster.shutdown();
		}

	}

}
