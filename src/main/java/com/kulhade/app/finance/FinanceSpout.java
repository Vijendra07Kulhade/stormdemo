package com.kulhade.app.finance;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by vn05f93 on 6/6/17.
 */
public class FinanceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
    }

    public void nextTuple() {
        try{
            StockQuote stockQuote = YahooFinance.get("CSCO").getQuote();
            BigDecimal price = stockQuote.getPrice();
            BigDecimal prevClose = stockQuote.getPreviousClose();
            Timestamp timeStamp = new Timestamp(System.currentTimeMillis());

            collector.emit(new Values(stockQuote.getSymbol(),price,prevClose,timeStamp));

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company","timestamp","price","prev_close"));
    }
}
