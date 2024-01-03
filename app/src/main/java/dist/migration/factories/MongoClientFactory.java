package dist.migration.factories;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

public class MongoClientFactory {

    public static MongoClient createClient(String host, String username, String password) {
        String credentials = "";
        if(!(username.isBlank() || password.isBlank())) { 
            credentials = username + ":" + password + "@";
        }
        String uri = "mongodb://" + credentials + host;
        return MongoClients.create(uri);
    }
}
