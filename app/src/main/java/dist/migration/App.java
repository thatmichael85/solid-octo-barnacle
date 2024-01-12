package dist.migration;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import dist.migration.dtos.EventType;
import dist.migration.dtos.InputDto;
import dist.migration.handlers.MongoMigrationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
    // Just for local testing
    public static void main(String[] args) {
        InputDto testInput = new InputDto();
        testInput.setDataBaseName("db_name1");
        testInput.setEventType(EventType.executeMigration);
        testInput.setCollectionName("yourCollectionName");
        testInput.setEnv("local");
        MongoMigrationHandler mongoMigrationHandlerLocal = new MongoMigrationHandler();
        LocalContext localContext = new LocalContext();
        String result = mongoMigrationHandlerLocal.handleRequest(testInput, localContext);
        log.info("Migration process completed in local testing with {}",result);
    }
}

class LocalContext implements Context {

    @Override
    public String getAwsRequestId() {
        return "LOCALTESTING";
    }

    @Override
    public ClientContext getClientContext() {
        return null;
    }

    @Override
    public String getFunctionName() {
        // TODO Auto-generated method stub
        return "LOCALTESTING";
    }

    @Override
    public String getFunctionVersion() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CognitoIdentity getIdentity() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getInvokedFunctionArn() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getLogGroupName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getLogStreamName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LambdaLogger getLogger() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getMemoryLimitInMB() {
        // TODO Auto-generated method stub
        return 10;
    }

    @Override
    public int getRemainingTimeInMillis() {
        // TODO Auto-generated method stub
        return 123407734;
    }
}