package com.kulhade.app;

import com.kulhade.app.finance.FinanceBolt;
import com.kulhade.app.finance.FinanceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by vn05f93 on 6/5/17.
 */
public class FinanceTopology {

    public static void main(String...args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Yahoo-finance-spout",new FinanceSpout());
        builder.setBolt("Yahoo-finance-bolt",new FinanceBolt())
        .shuffleGrouping("Yahoo-finance-spout");

        StormTopology stormTopology = builder.createTopology();

        Config config = new Config();
        config.setDebug(true);

//        LocalCluster localCluster = new LocalCluster();
        try {
            StormSubmitter.submitTopology("Yahoo-finance-topology",config,stormTopology);
//            Thread.sleep(100000);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
//            localCluster.shutdown();
        }

    }
}
