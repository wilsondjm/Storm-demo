package me.vincent.storm.demo2.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

public class RecordSpout extends BaseRichSpout {

    private static String[] records = {
            "123,234,20",
            "123,345,20",
            "345,234,20",
            "345,123,20",
            "234,123,20",
            "234,123,20",
            "123,234,20",
            "456,123,20",
            "456,234,20",
            "456,345,20",
            "123,456,20",
            "234,456,20",
            "345,456,20",
    };

    private SpoutOutputCollector outputCollector;

    private int index = 0;

    private Random random = new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void nextTuple() {
        String record = records[index];
        String[] words = record.split(",");
//        System.out.println(String.join("-", Arrays.asList(words)));

        outputCollector.emit(new Values(words[0], words[1], random.nextInt(100)));
        index = (index+1) % records.length;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("from", "to", "duration"));
    }
}
