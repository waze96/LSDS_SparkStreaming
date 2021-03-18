package upf.edu.storage;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import upf.edu.model.HashtagCompare;
import upf.edu.model.HashTagCount;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

import java.io.Serializable;
import java.util.*;

@SuppressWarnings("serial")
public class DynamoHashTagRepository implements IHashtagRepository, Serializable{
	
	final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
	final static String region = "us-east-1";
	final static String tableName = "LSDS2021-TwitterHashtags";
	
	// Create the client
	final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
			.withEndpointConfiguration(
					new AwsClientBuilder.EndpointConfiguration(endpoint, region)
			).withCredentials(new ProfileCredentialsProvider())
			.build();
	
	// Create the DB object and get the Table
	final DynamoDB dynamoDB = new DynamoDB(client);
	final Table dynamoDBTable = dynamoDB.getTable(tableName);

	@Override
	public void write(Status s) {		
		HashtagEntity[] entities = s.getHashtagEntities();
	    if (entities != null) {
	    	for (HashtagEntity he : entities) {
	    		if (he != null) {
    				String hashtag = he.getText().toString();
    				String lang = s.getLang().toString();
    				Long tweetId = s.getId();
    				// Try to update existing Hashtag
    				try {
	    				UpdateItemSpec updateItemSpec = new UpdateItemSpec()
	    						.withPrimaryKey("hashtag", hashtag, "language", lang)
	    		                .withUpdateExpression("set myCounter=myCounter+:b, tweetsId=list_append(:a,tweetsId)")
	    		                .withValueMap(new ValueMap().withNumber(":b",1).withList(":a", tweetId))
	    		                .withReturnValues(ReturnValue.UPDATED_NEW);
	
	    				UpdateItemOutcome outcome = dynamoDBTable.updateItem(updateItemSpec);
	    				
	    				System.out.println("Update succeeded:\n" + outcome.getUpdateItemResult());
    				} catch(Exception e) {
    					// Hashtag not exists, so add a new Item on the DB
	    				System.out.println("Adding new item");
	    				
	    				// Creates the Hashtag Item
	    				Item item = new Item()
	    						.withPrimaryKey("hashtag", hashtag, "language", lang)
	    						.withNumber("myCounter", 1)
	                            .withList("tweetsId", Arrays.asList(tweetId));
	    				
	    				// Add the Item on DB
    					dynamoDBTable.putItem(item);
    				}
	    		}
	    	}
	    }
	}





	  @SuppressWarnings("unchecked")
	@Override
	  public List<HashTagCount> readTop10(String lang) {
		  ArrayList<HashTagCount> list = new ArrayList<HashTagCount>();
	      ArrayList<HashTagCount> top10 = new ArrayList<HashTagCount>();

	      // Scanning whole DB, and get interesting attributes
	      ScanRequest scanRequest = new ScanRequest()
	              .withTableName(tableName)
	              .withAttributesToGet("hashtag","language","myCounter");
	      ScanResult result = client.scan(scanRequest);

	      // Parse the Items to get the correct value
	      for (Map<String, AttributeValue> item : result.getItems()) {
	            String counter = item.values().toArray()[0].toString().split("[ ]")[1].split("[,]")[0];
	            String language = item.values().toArray()[1].toString().split("[ ]")[1].split("[,]")[0];
	            String hashtag = item.values().toArray()[2].toString().split("[ ]")[1].split("[,]")[0];
	            
	            // Filter by language and add to a list
	            if(language.equals(lang)){
	              HashTagCount h = new HashTagCount(hashtag, language, Long.parseLong(counter));
	              list.add(h);
	            }
	      }
	      
	      //Sorting the hashtags using the java comparator class we created.
	      Collections.sort(list,new HashtagCompare());
	      
	      // Add the TOP 10 hashtags (if there are).
	      @SuppressWarnings("rawtypes")
	      Iterator itr=list.iterator();
	      int i=0;
	      while(itr.hasNext() && i<10){
	              top10.add((HashTagCount)itr.next());
	              i++;
	      }
	      
	      return top10;
	  }
}
