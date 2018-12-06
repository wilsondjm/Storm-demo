package me.vincent.storm.demo.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class ReportBolt extends BaseRichBolt {
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
        Long count = tuple.getLongByField("count");

        wordsCount.put(word, count);

        print();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void print(){
        System.out.println("---------------Report -------------");
        for(Map.Entry entry : wordsCount.entrySet()){
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        System.out.println("---------------       -------------");
    }
}
