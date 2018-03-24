package snapp.etl;

import snapp.etl.combiner.BaseCombinerEngine;
import snapp.etl.combiner.RideCombinerEngine;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main2 {
    public static void main(String[] args) {
        ExecutorService eventThreadPool = Executors.newFixedThreadPool(1);
        BaseCombinerEngine baseCombinerEngine = new RideCombinerEngine();
        eventThreadPool.execute(baseCombinerEngine);
        eventThreadPool.shutdownNow();

    }
}
