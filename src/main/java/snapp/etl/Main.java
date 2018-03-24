package snapp.etl;


import snapp.etl.db.KafkaReader;

public class Main {
    public static void main(String[] args) {
//todo :Change/add Connection initialization
        KafkaReader kafkaReader = new KafkaReader();
        kafkaReader.subscribe();
        System.out.println("done");


    }
}
