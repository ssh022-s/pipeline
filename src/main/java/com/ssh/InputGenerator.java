package com.ssh;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Date;
import java.util.Random;
/**
 * @author admin
 * @version 1.0.0
 * @ClassName InputGenerator.java
 * @Description TODO
 * @createTime 2022年12月19日 15:30:00
 */
public class InputGenerator {
    public static void main(String[] args) throws Exception {
        // Set the parameters
        int numPoints = 1000;
        float minTemperature = 20.0f;
        float maxTemperature = 30.0f;
        long interval = 60000;
        // 1 minute in milliseconds

        // Create a random number generator
        Random random = new Random();

        // Open the output file
        BufferedWriter writer = new BufferedWriter(new FileWriter("input.txt"));

        // Generate the input data
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < numPoints; i++) {
            float temperature = minTemperature + random.nextFloat() * (maxTemperature - minTemperature);
            writer.write(timestamp + "," + temperature + "\n");
            timestamp += interval;
        }

        // Close the output file
        writer.close();
    }
}
