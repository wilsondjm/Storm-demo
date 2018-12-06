package me.vincent.storm.demo2.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

public class RecordFormatBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String from = input.getStringByField("from");
        String to = input.getStringByField("to");
        int duration = input.getIntegerByField("duration");

        String key = String.format("%s-%s", from, to);
//        System.out.println(String.format("----Emitting----%s-%d", key, duration));

        outputCollector.emit(new Values(key, duration));
        outputCollector.emit(new Values(key, duration));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("recordKey", "duration"));
    }
}
