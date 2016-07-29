package com.hpe.rnd;

/**
 * Created by chzhenzh on 7/8/2016.
 */
public class Run {
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument...");
        } else {
            int p = 0;
            if (args[0].equals("producer")) {
                p = 1;
            } else if (args[0].equals("consumer")) {
                p = 2;
            }
            switch (p) {
                case 1:
                    LogProducer.main(args);
                    break;
                case 2:
                    LogConsumer.main(args);
                    break;
                default:
                    throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument...");
            }
        }
    }


}
