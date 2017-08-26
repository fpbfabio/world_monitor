//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;

import java.net.*;

//Create java class named “SimpleProducer”
public class SimpleProducer {

    private static Producer<String, String> producer;
    private static ServerSocket serverSocket ;
    private static String topicName;

    public static void initSocket()  throws Exception{
        int port = 3001;
        serverSocket = new ServerSocket(port);
    }

    public static void runServer()  throws Exception{
        initKafkaProducer();
        initSocket();
        while (true) {
            String str = "";
            System.out.println("accept1");
            Socket connectionSocket = serverSocket.accept();
            System.out.println("accept2");
            BufferedReader br = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            StringBuilder builder = new StringBuilder();
            while ((str = br.readLine()) != null) {
                System.out.println(str);
                builder.append(str);
            }
            producer.send(new ProducerRecord<String, String>(topicName, "" + System.currentTimeMillis(), builder.toString()));
            connectionSocket.close();
        }
    }

    public static void initKafkaProducer()  throws Exception{
        Properties props = new Properties();
        
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
        
        //Set acknowledgements for producer requests.      
        props.put("acks", "all");
        
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        
        //Specify buffer size in config
        props.put("batch.size", 16384);
        
        //Reduce the no of requests less than 0   
        props.put("linger.ms", 1);
        
        //The buffer.memory controls the total amount of memory available to the producer for buffering.   
        props.put("buffer.memory", 33554432);
        
        props.put("key.serializer", 
            "org.apache.kafka.common.serialization.StringSerializer");
            
        props.put("value.serializer", 
            "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer <String, String>(props);
    }

   public static void main(String[] args) throws Exception{
      
      // Check arguments length value
      if(args.length == 0){
         System.out.println("Enter topic name");
         return;
      }
      
      //Assign topicName to string variable
      SimpleProducer.topicName = args[0].toString();
      SimpleProducer.runServer();
   }
}
