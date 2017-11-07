import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import redis.clients.jedis.Jedis;
public class demo {
	
	//Db: mycustomers
	//Collection: customer
	//

	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		
		MongoClient mongo = new MongoClient( "localhost" , 27017 );
		DB db = mongo.getDB("SocialNetwork");
		System.out.println(db);
		
		
		Jedis NVM = new Jedis("localhost");
		System.out.println("Connection to server sucessfully. NVMCache is up and running"); 
		//check whether server is running or not 
		System.out.println("NVM is running: "+NVM.ping()); 
		  
		
//		List<String> dbs = mongo.getDatabaseNames();
//		for(String dbprint : dbs){
//			System.out.println(dbprint);
//		}
		
		
		DBCollection table = db.getCollection("Users");
		
//		int counter = 0;
//		while(counter < 50)
//		{
//			BasicDBObject document = new BasicDBObject();
//			String fname = "First_Name";
//			String lname = "Last_Name";
//			String fCount = "Friend_Count";
//			int fCountValue = 4;
//			String fList = "Friend_List";
//			String id = "id";
//			List<Integer> FriendList = new ArrayList<Integer>(); 
//			FriendList.add(1);
//			FriendList.add(2);
//			FriendList.add(3);
//			FriendList.add(4);
//			
//			document.put(fname, "John" + counter);
//			document.put(lname, "Doe" + counter);
//			document.put(fCount, fCountValue);
//			document.append(fList, FriendList);
//			document.put(id, counter);
//			table.insert(document);
//			//System.out.println(table.insert(document));
//			
//			NVM.set(fname+counter, "John" + counter);
//			NVM.set(lname+counter, "Doe"+counter);
//			NVM.set(fCount+counter, Integer.toString(fCountValue));
//			NVM.set(fList+counter, FriendList.toString());
//			NVM.set(id, Integer.toString(counter));
//			
//			counter++;
//		}
//		
//		System.out.println("This is it");
//		DBCursor curs = table.find(); 
//		Iterator<DBObject> fields = curs.iterator(); 
//		while(fields.hasNext()){ //
//		   BasicDBObject doc = (BasicDBObject)fields.next();
//		   System.out.println(doc.get("_id"));
//		} 
		
		
		
		
		List<String> ActionList = new ArrayList<String>();
		ActionList.add("First_Name");
		ActionList.add("Last_Name");
		ActionList.add("Friend_List");
		ActionList.add("Friend_Count");
		
		
		int counter = 80;
		
		Random randomizer = new Random();
		String key = ActionList.get(randomizer.nextInt(ActionList.size())) + counter ;
		
		String value = NVM.get(key);
		
		if(value==null)
		{
			
		}
		
		
		
		System.out.println(key);
		System.out.println(value);
		
	
		
	
		Jedis TSA = new Jedis("localhost"); 
		System.out.println("Connection to server sucessfully"); 
		//check whether server is running or not 
		System.out.println("TSA is running: "+TSA.ping()); 
		
		
	      
	      
	      
	}
	
	

}
