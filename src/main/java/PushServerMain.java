import com.rabbitmq.client.*;
import org.apache.commons.lang.SerializationUtils;
import product.Product;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by NIC on 6/11/17.
 */
public class PushServerMain {
    static final String mysql_host = "127.0.0.1:3306";
    static final String mysql_db = "project";
    static final String mysql_user = "root";
    static final String mysql_psw = "1127";
    
    public static void main(String args[]) throws IOException, TimeoutException {

        final MySQLAccess sqlAccess = new MySQLAccess(mysql_host, mysql_user, mysql_psw,mysql_db);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = null;

        try {
            connection = factory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare("LevelOne",true,false,false,null);

            final ArrayList<Product> reducedList = new ArrayList<>();

            ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(5);

            TestRunnable testRunnable = new TestRunnable(reducedList, sqlAccess);

            scheduledThreadPool.scheduleAtFixedRate(testRunnable,1,30, TimeUnit.SECONDS);

            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {

                    Product product = (Product) SerializationUtils.deserialize(body);
                    System.out.println(product.productId);
                    reducedList.add(product);


                }
            };

            channel.basicConsume("LevelOne", true, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }




    }
}
