package me.vincent.storm.demo.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    private Map<String, Long> wordsCount = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        wordsCount = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        long count = wordsCount.getOrDefault(word, 0L);
        wordsCount.put(word, ++count);

        outputCollector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
