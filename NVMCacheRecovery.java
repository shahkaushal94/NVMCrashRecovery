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
public class NVMCacheRecovery {

	static int counter=0;
	static long final_time1=0;

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
		

		for(int i=0;i<3000;i++)
		{
			 int randno =  (int)(Math.random() * ((2) + 1));
			String actiontoperform=basic_actions.get(randno);
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
		}
		

		System.out.println("RESPONSE TIME TO SERVICE REQUESTS FROM CACHE (3000 Requests) BEFORE NVM is DOWN is "+final_time1/Math.pow(10, 6)+"ms");

		
		long start_time2=System.nanoTime();
		//DOWNTIME NOW STARTS
		for(int i=0;i<3000;i++)
		{
			int randno =  (int)(Math.random() * ((2) + 1));
			String actiontoperform=basic_actions.get(randno);
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
		}
		final_time1=0;
		long start=System.nanoTime();
		
		recovery(NVM,TSA);
		
		long end=System.nanoTime();
		final_time1+=(end-start);
		
		System.out.println("RECOVERY TIME AFTER 3000 REQUESTS IS "+final_time1/Math.pow(10, 6)+"ms");
		final_time1=0;
		
		for(int i=0;i<3000;i++)
		{
			int randno =  (int)(Math.random() * ((2) + 1));
			String actiontoperform=basic_actions.get(randno);
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
		}
		

		System.out.println("RESPONSE TIME TO SERVICE 3000 Requests AFTER NVM is DOWN is "+final_time1/Math.pow(10, 6)+"ms");

	}
	public static void recovery(Jedis NVM,Jedis TSA)
	{
		for(String x:TSA.keys("*"))
		{
			NVM.set(x,TSA.get(x));
		}
	}
	public static void insert(Jedis Redis_Instance,DBCollection collection,List<String> ActionList)
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

		Redis_Instance.set(fname+"_"+counter, "John" + counter);
		Redis_Instance.set(lname+"_"+counter, "Doe"+counter);
		Redis_Instance.set(fCount+"_"+counter, Integer.toString(fCountValue));
		Redis_Instance.set(fList+"_"+counter, FriendList.toString());
		Redis_Instance.set(id, Integer.toString(counter));
		//System.out.println("NEW DOCUMENT ADDED "+document.toString());
		counter++;
	}
	public static void set(Jedis Redis_Instance,DBCollection collection,List<String> ActionList)
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
		int newfriendnumber=r.nextInt(counter);
		friend_list.add(newfriendnumber);
		int friend_count=(int) current.get("Friend_Count");
		friend_count++;
		
		current.put("Friend_Count",friend_count);
		current.put("Friend_List",friend_list);
		
		BasicDBObject whereQuery2 = new BasicDBObject();
		whereQuery.put("id", newfriendnumber);
		DBCursor cursor2 = collection.find(whereQuery);
		BasicDBObject current2=(BasicDBObject) cursor2.next();

		//System.out.println("BEFORE "+current.toString());
		ArrayList<Integer> friend_list2=(ArrayList<Integer>) current2.get("Friend_List");
		friend_list2.add(number_id);
		int friend_count2=(int) current.get("Friend_Count");
		friend_count2++;
		
		current2.put("Friend_Count",friend_count2);
		current2.put("Friend_List",friend_list2);

		Redis_Instance.set("Friend_Count_"+number_id,Integer.toString(friend_count));
		Redis_Instance.set("Friend_List_"+number_id,friend_list.toString());
		
		
		Redis_Instance.set("Friend_Count_"+newfriendnumber,Integer.toString(friend_count2));
		Redis_Instance.set("Friend_List_"+newfriendnumber,friend_list2.toString());
		//System.out.println("AFTER "+current.toString());
	}
	public static void get(Jedis Redis_Instance,DBCollection collection,List<String> ActionList)
	{
		Random r=new Random();
		//System.out.println("we are in GET");
		int number_id=r.nextInt(counter);
		String parameter=ActionList.get(r.nextInt(ActionList.size()));
		String checkquery=parameter+"_"+number_id;
		long start=System.nanoTime();
		String query_result=Redis_Instance.get(checkquery);
		long end=System.nanoTime();
		if(query_result==null)
		{
			//System.out.println("NVM Miss");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("id", number_id);
			DBCursor cursor = collection.find(whereQuery);
			String ans=cursor.next().get(parameter).toString();
			//System.out.println("VALUE FOR THIS ID "+number_id+" IS "+ans);
			Redis_Instance.set(checkquery, ans);
		}
		else
		{
			final_time1+=(end-start);
			//System.out.println("NVM Hit");
			//System.out.println("VALUE FOR THIS ID "+number_id+" IS "+query_result);
		}
	}
}