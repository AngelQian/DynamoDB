import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

/**
 * Created by AngelQian on 3/2/16.
 */
public class db implements Idb {

    public static String ID = "Id";
    public static String NAME = "Name";

    static AmazonDynamoDBClient client = new AmazonDynamoDBClient()
            .withEndpoint("http://localhost:8000");

    static DynamoDB dynamoDB = new DynamoDB(client);

    static String tableName = "ExampleTable7";

    public void createTB() {
        System.out.println("LOG ------- createTB");
        try {
            // A key schema specifies the attributes that make up the primary key of a table, or the key attributes of an index.
            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(ID).withKeyType(KeyType.HASH)); //Partition key
            keySchema.add(new KeySchemaElement().withAttributeName(NAME).withKeyType(KeyType.RANGE));

            //AtributeDefinition represents an attribute for describing the key schema for the table and indexes.
            ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(ID).withAttributeType("N"));
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(NAME).withAttributeType("S"));

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(new ProvisionedThroughput()
                            .withReadCapacityUnits(10L)
                            .withWriteCapacityUnits(10L));

            System.out.println("Issuing CreateTable request for " + tableName);
            Table table = dynamoDB.createTable(request);

            System.out.println("Waiting for " + tableName
                    + " to be created...this may take a while...");
            table.waitForActive();

            aboutTB();

        } catch (Exception e) {
            System.err.println("CreateTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    public void deleteTB() {
        System.out.println("LOG ------- deleteTB");
        Table table = dynamoDB.getTable(tableName);
        try {
            System.out.println("Issuing DeleteTable request for " + tableName);
            table.delete();

            System.out.println("Waiting for " + tableName
                    + " to be deleted...this may take a while...");

            table.waitForDelete();
        } catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }

    public void aboutTB() {
        System.out.println("Describing " + tableName);

        TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
        System.out.format("Name: %s,\n" + "Status: %s, \n"
                        + "Provisioned Throughput (read capacity units/sec): %d, \n"
                        + "Provisioned Throughput (write capacity units/sec): %d, \n",
                tableDescription.getTableName(),
                tableDescription.getTableStatus(),
                tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
    }

    public void listAllTBs() {
        TableCollection<ListTablesResult> tables = dynamoDB.listTables();
        Iterator<Table> iterator = tables.iterator();

        System.out.println("Listing table names");

        while (iterator.hasNext()) {
            Table table = iterator.next();
            System.out.println(table.getTableName());
        }
    }

    ////////
    Number id = UUID.randomUUID().hashCode();
    String name = "abc";
    public void insertData() {
        System.out.println("LOG ------- insertData");
//        Number id = UUID.randomUUID().hashCode();

        Table table = dynamoDB.getTable(tableName);
        try {
            System.out.println("Adding a new item...");
            Item item = new Item();
            item.withPrimaryKey(ID, id, NAME, name);

            item.withJSON("append","{\"info\" : \"ha pi.\"}");

            PutItemOutcome outcome = table.putItem(item);

            System.out.println("****PutItem succeeded : " + outcome/*getItem().toJSONPretty());//*/.getPutItemResult());


        } catch (Exception e) {
            System.err.println("Unable to add item: " + id + " " + name);
            System.err.println(e.getMessage());
        }
    }

    public void updateData(String updateExp, String updateKey, String updateValue) {
        System.out.println("LOG ------- updateData");

        UpdateItemSpec upitem = new UpdateItemSpec()
        .withPrimaryKey(ID, id, NAME, name)
        .withUpdateExpression(updateExp)
        .withValueMap(new ValueMap().withString(updateKey, updateValue)) // withNumber, withString, withList
        .withReturnValues(ReturnValue.UPDATED_NEW);

        Table table = dynamoDB.getTable(tableName);
        try {
            System.out.println("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(upitem);
            System.out.println("****UpdateItem succeeded:" + outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Unable to update item: " + id + " " + name);
            System.err.println(e.getMessage());
        }
    }

    public void deleteData(String deleteExp, String deleteKey, String deleteVal) {
        System.out.println("LOG ------- deleteData");

        Table table = dynamoDB.getTable(tableName);

        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey(ID, id, NAME, name)
                .withConditionExpression(deleteExp)
                .withValueMap(new ValueMap().withString(deleteKey, deleteVal));


        try {
            System.out.println("Attempting a conditional delete...");
            table.deleteItem(deleteItemSpec);
            System.out.println("****DeleteItem succeeded");
        } catch (Exception e) {
            System.err.println("Unable to delete item: " + name + " " + name);
            System.err.println(e.getMessage());
        }
    }

    public void queryAllData() {
        System.out.println("LOG ------- queryAllData");

        Table table = dynamoDB.getTable(tableName);

        ScanSpec scanSpec = new ScanSpec();
                /*.withProjectionExpression("#yr, title, info.rating")
                .withFilterExpression("#yr between :start_yr and :end_yr")
                .withNameMap(new NameMap().with("#yr",  "year"))
                .withValueMap(new ValueMap().withNumber(":start_yr", 1950).withNumber(":end_yr", 1959));*/

        try {
            ItemCollection<ScanOutcome> items = table.scan(scanSpec);

            Iterator<Item> iter = items.iterator();
            while (iter.hasNext()) {
                Item item = iter.next();
                System.out.println(item.toJSONPretty()/*.toString()*/);
            }

        } catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }
    }

    public void queryData(String queryExp, String queryKey, String queryVal) {
        System.out.println("LOG ------- queryData");

        Table table = dynamoDB.getTable(tableName);

        /* query by id */
        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#i = :nm")
                .withNameMap(new NameMap().with("#i", "Id"))
                .withValueMap(new ValueMap().withNumber(":nm",1680809341));

        /*QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("#ai = :val")
                .withNameMap(new NameMap().with("#ai", "append.info"))
//                .withFilterExpression("append.info = :val")
                .withValueMap(new ValueMap().withString(":val","be happy"))
                ;*/

        ItemCollection<QueryOutcome> items;
        Iterator<Item> iterator;
        Item item;

        try {
            System.out.println("info is happy");
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                System.out.println(item.getString(NAME) + ": " + item.getString(ID));
            }

        } catch (Exception e) {
            System.err.println("Unable to query info");
            System.err.println(e.getMessage());
        }
    }

}
