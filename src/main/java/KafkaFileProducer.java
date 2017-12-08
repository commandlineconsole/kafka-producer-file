import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaFileProducer extends Thread {

    private final KafkaProducer<String, String> producer;
    private final Boolean isAsync;

    public KafkaFileProducer(String topic, Boolean isAsync) {
        Properties props = new Properties();
        //props.put("bootstrap.servers", "10.1.51.39:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "BiddataProducer");
        props.put("batch.size", "10");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        this.isAsync = isAsync;
    }

    public void sendMessage(String key, String value, String args[]) {
        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(
                    new ProducerRecord<String, String>(args[0].toString(), key),
                    (Callback) new DemoCallBack(startTime, key, value));
        } else { // Send synchronously
            try {
                producer.send(
                        new ProducerRecord<String, String>(args[0].toString(), key, value))
                        .get();
                System.out.println("Sent message: (" + key + ", " + value + ")");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String [] args){
        String topicName = args[0].toString();
        String fileName = args[1].toString();
        KafkaFileProducer producer = new KafkaFileProducer(topicName, false);
        int lineCount = 0;
        FileInputStream fis;
        BufferedReader br = null;
        try {
            fis = new FileInputStream(fileName);
            //Construct BufferedReader from InputStreamReader
            br = new BufferedReader(new InputStreamReader(fis));
//            String outPutFile = fileName+"out.txt";
//            File file = new File(outPutFile);
//            if (!file.exists()) {
//                file.createNewFile();
//            }
//            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));

            String line = null;
            String header = br.readLine();
//            StringBuffer fileContent = new StringBuffer("");
            while ((line = br.readLine()) != null) {
                lineCount++;

                String[] columns = line.replace(",",",`").split(",");
                    StringBuilder formatedLine = new StringBuilder("");
                    for (String column : columns) {
                        column = column.contains("`") ? column.substring(1,column.length()) : column;
                        formatedLine.append(column.equals("") ? "0" : column).append(",");
                    }
                    if (formatedLine.length() > 0) {
                        line = formatedLine.substring(0, formatedLine.length()-1);
                    }
                producer.sendMessage(lineCount+"", line,args);
//                fileContent.append(line);
            }
//            bw.write(fileContent.toString());
//            bw.close();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            try {
                br.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
}

class DemoCallBack implements Callback {

    private long startTime;
    private String key;
    private String message;

    public DemoCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling
     * of request completion. This method will be called when the record sent to
     * the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata
     *            The metadata for the record that was sent (i.e. the partition
     *            and offset). Null if an error occurred.
     * @param exception
     *            The exception thrown during processing of this record. Null if
     *            no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println("message(" + key + ", " + message
                    + ") sent to partition(" + metadata.partition() + "), "
                    + "offset(" + metadata.offset() + ") in " + elapsedTime
                    + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}