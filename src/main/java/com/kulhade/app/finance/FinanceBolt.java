package com.kulhade.app.finance;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by vn05f93 on 6/6/17.
 */
public class FinanceBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String symbol = tuple.getStringByField("company");
        String timestamp = tuple.getStringByField("timestamp"); //Can be done like tuple.getStringByField("timestamp")

        Double price = Double.valueOf(tuple.getStringByField("price"));
        Double prevClose = Double.valueOf(tuple.getStringByField("prev_close"));

        Boolean gain=true;
        if(prevClose>=price){
            gain=false;
        }

        basicOutputCollector.emit(new Values(symbol,price,gain?"gain":"loss"));

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company","price","gain/loss"));
    }
}
