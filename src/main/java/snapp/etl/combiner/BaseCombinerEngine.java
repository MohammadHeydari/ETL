package snapp.etl.combiner;


import snapp.etl.util.Configuration;

public abstract class BaseCombinerEngine implements Runnable {
    static protected Configuration configuration = Configuration.getInstance();
    static private long interval = 0;

    public BaseCombinerEngine() {
        interval = setInterval();
    }

    abstract void engine();

    abstract long setInterval();

    abstract void failureStatements();

    @Override
    public void run() {
        while (true) {
            engine();
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                failureStatements();
            }
        }
    }
}
