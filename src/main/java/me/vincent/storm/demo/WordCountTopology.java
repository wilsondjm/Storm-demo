package me.vincent.storm.demo;

import me.vincent.storm.demo.bolt.CountBolt;
import me.vincent.storm.demo.bolt.ReportBolt;
import me.vincent.storm.demo.bolt.SplitBolt;
import me.vincent.storm.demo.spout.WordsSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    public static void main(String[] args) {
        WordsSpout wordsSpout = new WordsSpout();

        ReportBolt reportBolt = new ReportBolt();
        SplitBolt splitBolt = new SplitBolt();
        CountBolt countBolt = new CountBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("wordspout", wordsSpout, 2);
        topologyBuilder.setBolt("wordsplit", splitBolt).shuffleGrouping("wordspout");
        topologyBuilder.setBolt("wordcount", countBolt).fieldsGrouping("wordsplit", new Fields("word"));
        topologyBuilder.setBolt("boltreport", reportBolt).globalGrouping("wordcount");

        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("word-count", config, topologyBuilder.createTopology());
    }
}
