package wordcount;

import static utils.Utils.waitForSeconds;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

	private static final String SENTENCE_SPOUT_ID = "sentence_spout";
	private static final String SPLIT_BOLT_ID = "split_bolt";
	private static final String COUNT_BOLT_ID = "count_bolt";
	private static final String REPORT_BOLT_ID = "report_bolt";
	private static final String TOPOLOGY_NAME = "word_count";

	public static void main(String[] args) throws Exception, InvalidTopologyException, AuthorizationException {

		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		// SentenceSpout --> SplitSentenceBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
		// SplitSentenceBolt --> WordCountBolt
		builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		// WordCountBolt --> ReportBolt
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

		Config config = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		waitForSeconds(3);
		cluster.killTopology(TOPOLOGY_NAME);

		cluster.shutdown();
	}

}
