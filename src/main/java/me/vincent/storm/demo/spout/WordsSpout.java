package me.vincent.storm.demo.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordsSpout extends BaseRichSpout {

    private static String[] messages = {
            "The name Excalibur ultimately derives from the Welsh Caledfwlch (and Breton Kaledvoulc'h, Middle Cornish Calesvol) which is a compound of caled \"hard\" and bwlch \"breach, cleft\".[1] Caledfwlch appears in several early Welsh works, including the prose tale Culhwch and Olwen. The name was later used in Welsh adaptations of foreign material such as the Bruts (chronicles), which were based on Geoffrey of Monmouth.",
            "n Old French sources this then became Escalibor, Excalibor, and finally the familiar Excalibur. Geoffrey Gaimar, in his Old French L'Estoire des Engleis (1134-1140), mentions Arthur and his sword: \"this Constantine was the nephew of Arthur, who had the sword Caliburc\" (\"Cil Costentin, li niès Artur, Ki out l'espée Caliburc\").[5][6] In Wace's Roman de Brut (c. 1150-1155), an Old French translation and versification of Geoffrey's Historia, the sword is called Calabrum, Callibourc, Chalabrun, and Calabrun (with alternate spellings such as Chalabrum, Calibore, Callibor, Caliborne, Calliborc, and Escaliborc, found in various manuscripts of the Brut)",
            "In Arthurian romance, a number of explanations are given for Arthur's possession of Excalibur. In Robert de Boron's Merlin, the first tale to mention the \"sword in the stone\" motif, Arthur obtained the British throne by pulling a sword from an anvil sitting atop a stone that appeared in a churchyard on Christmas Eve.[12][note 1] In this account, the act could not be performed except by \"the true king,\" meaning the divinely appointed king or true heir of Uther Pendragon. As Malory writes: \"Whoso pulleth out this sword of this stone and anvil, is rightwise king born.\"[13][note 2] This sword is thought by many to be the famous Excalibur, and its identity is made explicit in the later Prose Merlin, part of the Lancelot-Grail cycle.[14] The challenge of drawing a sword from a stone also appears in the Arthurian legends of Galahad, whose achievement of the task indicates that he is destined to find the Holy Grail"
    };

    private int index = 0;

    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(new Values(messages[index]));

        index = (index + 1) % messages.length;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }
}
