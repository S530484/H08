package com.spnotes.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by sunilpatil on 12/28/15.
 */
public class Consumer {
    private static Scanner in;

    public static void main(String[] argv)throws Exception{

        if (argv.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    Consumer.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String topicName = argv[0];
        String groupId = argv[1];

        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String topicName;
        private String groupId;
        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId){
            this.topicName = topicName;
            this.groupId = groupId;
        }
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            //Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            //Start processing messages
        String[] str=new String[50];
        str[0]="Marshmallow Treat";
        str[1]="Mocha Almond Fudge";
        str[2]="Valencia Orange Ice";
        str[3]="Toasted Almond";
        str[4]="Saffron Pistachio";
        str[5]="Banana Coconut Macaroon Cheesecake";
        str[6]="Caramel Apple Crunch";
        str[7]="Chocolate Brownie Chunks";
        str[8]="German Chocolate Cheesecake";
        str[9]="Hazelnut Coffee";
        str[10]="Honey Espresso";
        str[11]="Lemon Blueberry Cheesecake";
        str[12]="Mint Chocolate Chip";
        str[13]="Peanut Butter Fudge Cheesecake";
        str[14]="Pineapple Cheesecake";
        str[15]="Raspberry Chocolate Truffle";
        str[16]="Snickers Caramel Cheesecake";
        str[17]="Tiramisu Toffee Cheesecake";
        str[18]="Vanilla Cookies & Cream";
        str[19]="White Chocolate Raspberry Truffle";
        str[20]="Wild Cherry";
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                    {
                        for(int i=0; i<50; i++)
                        {
                            if(record.value().equals(String.valueOf(i)))
                            {
                                System.out.println("Your favourite ice cream is: "+str[i]);
                            }
                        }
                    }
                }
            }catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            }finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,String> getKafkaConsumer(){
           return this.kafkaConsumer;
        }
    }
}

