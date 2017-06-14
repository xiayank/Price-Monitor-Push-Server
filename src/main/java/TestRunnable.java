import net.spy.memcached.MemcachedClient;
import product.Product;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * Created by NIC on 6/13/17.
 */
public class TestRunnable implements Runnable {
    private static final long serialVersionUID = 1L;
    static final String mysql_host = "127.0.0.1:3306";
    static final String mysql_db = "project";
    static final String mysql_user = "root";
    static final String mysql_psw = "1127";
    ArrayList<Product> reducedList = null;
    MySQLAccess sqlAccess = null;

    public TestRunnable(ArrayList<Product> reducedList, MySQLAccess sqlAccess){
       this.reducedList = reducedList;
       this.sqlAccess = sqlAccess;
    }

    @Override
    public void run() {
        System.out.println("60 s run again!!!!");
        for(Product product : reducedList){
            double newPrice = product.newPrice;
            MemcachedClient cache = null;
            try {
                cache = new MemcachedClient(new InetSocketAddress("127.0.0.1",11211));
            } catch (IOException e) {
                e.printStackTrace();
            }

            if(cache.get(product.productId) instanceof Double){
                //get old Price from cache
                double cachedPrice = (double) cache.get(product.productId);

                //Price has changed, update database and cache
                if(cachedPrice != newPrice){
                    //1.update DB: oldPice = cacahedPrice,newPrice = newPrice
                    try {
                        sqlAccess.updatePrice(product.productId, cachedPrice, newPrice);
                        System.out.println("update product" + product.productId + " " + product.newPrice);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    //2.update cached price
                    cache.set(product.productId, 72000, newPrice);
                }




                //Current product not exist, add it into DB and cache
            }else {
                //set cache
                cache.set(product.productId, 72000, newPrice);
                //set database

                try {
                    sqlAccess.addProductData(product);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Add into database:Id  -->" + product.productId);
                System.out.println("Add into database: Price --> " + product.newPrice);



            }
        }
        reducedList.clear();
    }

}
