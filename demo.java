package Project;
import java.net.UnknownHostException;
import java.util.*;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import redis.clients.jedis.Jedis;
public class demo {

	static int counter=0;
	static double hit_ratio=0;

	public static void main(String[] args) throws UnknownHostException {
		// TODO Auto-generated method stub
		//Initial setup factors MongoDb and Redis
		Random r=new Random();
		MongoClient mongo = new MongoClient( "localhost" , 27017 );
		DB db = mongo.getDB("SocialNetwork");
		//System.out.println(db);



		List<String> ActionList = new ArrayList<String>();
		ActionList.add("First_Name");
		ActionList.add("Last_Name");
		ActionList.add("Friend_List");
		ActionList.add("Friend_Count");


		List<String> basic_actions = new ArrayList<String>();
		basic_actions.add("GET");
		basic_actions.add("SET");
		basic_actions.add("INSERT");


		Jedis NVM = new Jedis("localhost");
		//System.out.println("Connection to server sucessfully. NVMCache is up and running"); 
		//check whether server is running or not 
		//System.out.println("NVM is running: "+NVM.ping()); 

		Jedis TSA = new Jedis("localhost"); 
		//System.out.println("Connection to server sucessfully"); 
		//check whether server is running or not 
		//System.out.println("TSA is running: "+TSA.ping()); 

		DBCollection collection = db.getCollection("Users");

		NVM.flushDB();

		while(counter < 1000)
		{
			BasicDBObject document = new BasicDBObject();
			String fname = "First_Name";
			String lname = "Last_Name";
			String fCount = "Friend_Count";
			int fCountValue = 4;
			String fList = "Friend_List";
			String id = "id";
			List<Integer> FriendList = new ArrayList<Integer>(); 
			int c=0;
			while(c!=4)
			{
				FriendList.add(r.nextInt(10000));
				c++;
			}

			document.put(fname, "John" + counter);
			document.put(lname, "Doe" + counter);
			document.put(fCount, fCountValue);
			document.append(fList, FriendList);
			document.put(id, counter);
			collection.insert(document);

			NVM.set(fname+"_"+counter, "John" + counter);
			NVM.set(lname+"_"+counter, "Doe"+counter);
			NVM.set(fCount+"_"+counter, Integer.toString(fCountValue));
			NVM.set(fList+"_"+counter, FriendList.toString());
			NVM.set(id, Integer.toString(counter));

			counter++;
		}



		int actioncounter=0;
		long start_time1=System.nanoTime();

		while(actioncounter<1000)
		{
			String actiontoperform=basic_actions.get(r.nextInt(basic_actions.size()));
			if(actiontoperform.equals("GET"))
			{
				get(NVM,collection,ActionList);
			}
			else if(actiontoperform.equals("SET"))
			{
				set(NVM, collection, ActionList);

			}
			else if(actiontoperform.equals("INSERT"))
			{
				insert(NVM, collection, ActionList);
			}
			actioncounter++;
		}
		long end_time1=System.nanoTime();
		long elapsed_time1=end_time1-start_time1;
		hit_ratio/=1000;
		System.out.println("HIT RATIO IS : "+hit_ratio);
		System.out.println("elapsed time for UPTIME is "+elapsed_time1/Math.pow(10, 6));
		hit_ratio=0;
		
		long start_time2=System.nanoTime();
		//DOWNTIME NOW STARTS
		actioncounter=0;	
		while(actioncounter<1000)
		{
			String actiontoperform=basic_actions.get(r.nextInt(basic_actions.size()));
			if(actiontoperform.equals("GET"))
			{
				get(TSA,collection,ActionList);
			}
			else if(actiontoperform.equals("SET"))
			{
				set(TSA, collection, ActionList);
			}
			else if(actiontoperform.equals("INSERT"))
			{
				insert(TSA, collection, ActionList);
			}
			actioncounter++;
		}

		recovery(NVM,TSA);
		
		
		
		long end_time2=System.nanoTime();
		long elapsed_time2=end_time2-start_time2;
		
		
		hit_ratio/=1000;
		System.out.println("HIT RATIO IS : "+hit_ratio);
		System.out.println("elapsed time for DOWNTIME is "+elapsed_time2/Math.pow(10, 6));

	}
	public static void recovery(Jedis NVM,Jedis TSA)
	{
		for(String x:TSA.keys("*"))
		{
			NVM.set(x,TSA.get(x));
		}
	}
	public static void insert(Jedis NVM,DBCollection collection,List<String> ActionList)
	{
		Random r=new Random();
//		//System.out.println("we are in INSERT");
		BasicDBObject document = new BasicDBObject();
		String fname = "First_Name";
		String lname = "Last_Name";
		String fCount = "Friend_Count";
		int fCountValue = 4;
		String fList = "Friend_List";
		String id = "id";
		List<Integer> FriendList = new ArrayList<Integer>(); 
		int c=0;
		while(c!=4)
		{
			FriendList.add(r.nextInt(10000));
			c++;
		}

		document.put(fname, "John" + counter);
		document.put(lname, "Doe" + counter);
		document.put(fCount, fCountValue);
		document.append(fList, FriendList);
		document.put(id, counter);
		collection.insert(document);

		NVM.set(fname+"_"+counter, "John" + counter);
		NVM.set(lname+"_"+counter, "Doe"+counter);
		NVM.set(fCount+"_"+counter, Integer.toString(fCountValue));
		NVM.set(fList+"_"+counter, FriendList.toString());
		NVM.set(id, Integer.toString(counter));
		//System.out.println("NEW DOCUMENT ADDED "+document.toString());
		counter++;
	}
	public static void set(Jedis NVM,DBCollection collection,List<String> ActionList)
	{
		Random r=new Random();
		//System.out.println("we are in SET");
		int number_id=r.nextInt(counter);

		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("id", number_id);
		DBCursor cursor = collection.find(whereQuery);
		BasicDBObject current=(BasicDBObject) cursor.next();

		//System.out.println("BEFORE "+current.toString());
		ArrayList<Integer> friend_list=(ArrayList<Integer>) current.get("Friend_List");
		friend_list.add(r.nextInt(10000));
		int friend_count=(int) current.get("Friend_Count");
		friend_count++;
		current.put("Friend_Count",friend_count);
		current.put("Friend_List",friend_list);

		NVM.set("Friend_Count_"+number_id,Integer.toString(friend_count));
		NVM.set("Friend_List_"+number_id,friend_list.toString());
		//System.out.println("AFTER "+current.toString());
	}
	public static void get(Jedis NVM,DBCollection collection,List<String> ActionList)
	{
		Random r=new Random();
		//System.out.println("we are in GET");
		int number_id=r.nextInt(counter);
		String parameter=ActionList.get(r.nextInt(ActionList.size()));
		String checkquery=parameter+"_"+number_id;
	
		String query_result=NVM.get(checkquery);
		if(query_result==null)
		{
			//System.out.println("NVM Miss");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("id", number_id);
			DBCursor cursor = collection.find(whereQuery);
			String ans=cursor.next().get(parameter).toString();
			//System.out.println("VALUE FOR THIS ID "+number_id+" IS "+ans);
			NVM.set(checkquery, ans);
		}
		else
		{
			hit_ratio++;
			//System.out.println("NVM Hit");
			//System.out.println("VALUE FOR THIS ID "+number_id+" IS "+query_result);
		}
	}
}