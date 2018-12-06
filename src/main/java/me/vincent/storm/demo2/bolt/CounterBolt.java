package me.vincent.storm.demo2.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CounterBolt implements IRichBolt {

    private OutputCollector outputCollector;

    private Map<String, Integer> counts;
    private Map<String, Integer> duration;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        counts = new HashMap<>();
        duration = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String recordKey = input.getString(0);
        int durationTime = input.getInteger(1);
//        System.out.println(String.format("----accu----%s-%d", recordKey, durationTime));

        int countsTotal = counts.getOrDefault(recordKey, 0) + 1;
        counts.put(recordKey, countsTotal);

        int durationTimeTotal = duration.getOrDefault(recordKey, 0);
        int newDurationTimeTotal = durationTimeTotal + durationTime;
        duration.put(recordKey, newDurationTimeTotal);

        outputCollector.ack(input);
    }

    @Override
    public void cleanup() {

        for(String key : counts.keySet()){
            int count = counts.get(key);
            int durationTotal = duration.get(key);
            System.out.println(String.format("****************%s:%d,%d", key, count, durationTotal));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
