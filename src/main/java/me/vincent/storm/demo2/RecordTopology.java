package me.vincent.storm.demo2;

import me.vincent.storm.demo2.bolt.CounterBolt;
import me.vincent.storm.demo2.bolt.RecordFormatBolt;
import me.vincent.storm.demo2.spout.RecordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class RecordTopology {

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
//        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("record-spout", new RecordSpout(), 2);
        builder.setBolt("record-format-bolt", new RecordFormatBolt(),10).shuffleGrouping("record-spout");
        builder.setBolt("record-counter-bolt", new CounterBolt(),3).fieldsGrouping("record-format-bolt", new Fields("recordKey"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RecordAnalysis", config, builder.createTopology());

        Thread.sleep(10000);

        cluster.shutdown();
    }
}
