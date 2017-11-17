package mongoDB;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.UpdateResult;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MongoBGClient extends DB {

	MongoClient mongoClient;
	private String ipAddress;
	
//	static JedisPool pool1 = new JedisPool(new JedisPoolConfig(), "localhost",6379);
//	static JedisPool pool2 = new JedisPool(new JedisPoolConfig(), "localhost",6380);
	
	
	Jedis NVM=new Jedis("localhost",6380);
	Jedis TSA=new Jedis("localhost",6379);
//	Jedis NVM=pool2.getResource();
	static boolean isRecovery=false;
	
//	Jedis TSA=pool1.getResource();
	static volatile int NvmIsUp=1;
	static volatile AtomicBoolean failedmode=new AtomicBoolean(false);
	boolean NVMinRecovery=false;
	static AtomicBoolean first_time=new AtomicBoolean(true);
	
	
	public static final String MONGO_DB_NAME = "BG";
	public static final String MONGO_USER_COLLECTION = "users";
	public static final String KEY_FRIEND = "f";
	public static final String KEY_PENDING = "p";
	public static final String KEY_MONGO_DB_IP = "mongoip";

	public static final int LIST_FRIENDS = 10;
	
	public static AtomicBoolean friendLoad = new AtomicBoolean(false);
	public static AtomicBoolean createFriendship = new AtomicBoolean(false);
	public static final Semaphore loadFriends = new Semaphore(1);
	
	public static int isNVM=1; 
	public boolean isFailureStarted=false;
	

	public MongoBGClient(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	public MongoBGClient() {
		java.util.logging.Logger mongoLogger = java.util.logging.Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(Level.SEVERE);
	}

	private final Logger log = Logger.getLogger(MongoBGClient.class);

	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		Document doc = new Document();
		doc.put("_id", entityPK);
		values.forEach((k, v) -> {
			doc.put(k, v.toString());
		});
		doc.put(KEY_FRIEND, new HashSet<Integer>());
		doc.put(KEY_PENDING, new HashSet<Integer>());
		coll.insertOne(doc);
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		
		
//		NVM.flushDB();
		
//		
//		HashSet<String> RedisHsetField = new HashSet<>();
//		RedisHsetField.add("f");
//		RedisHsetField.add("p");
//		RedisHsetField.add("username");
//		RedisHsetField.add("pw");
//		RedisHsetField.add("fname");
//		RedisHsetField.add("lname");
//		RedisHsetField.add("gender");
//		RedisHsetField.add("jdate");
//		RedisHsetField.add("ldate");
//		RedisHsetField.add("address");
//		RedisHsetField.add("email");
//		RedisHsetField.add("tel");
		
		
		if(NvmIsUp==1)
		{
		String value = NVM.get(Integer.toString(profileOwnerID));
		
		if(value==null)
		{
		//System.out.println("Cache Miss");
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

	
		
		List<Bson> queries = new ArrayList<Bson>();
		queries.add(new BasicDBObject("$match",
				new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
		
		//System.out.println("Queries" + queries);
		
		
		BasicDBObject obj = new BasicDBObject();
		obj.put("f", new BasicDBObject("$size", "$f"));
		obj.put("p", new BasicDBObject("$size", "$p"));
		obj.put("username", 1);
		obj.put("pw", 1);
		obj.put("fname", 1);
		obj.put("lname", 1);
		obj.put("gender", 1);
		obj.put("dob", 1);
		obj.put("jdate", 1);
		obj.put("ldate", 1);
		obj.put("address", 1);
		obj.put("email", 1);
		obj.put("tel", 1);
		BasicDBObject bobj = new BasicDBObject("$project", obj);
		queries.add(bobj);

		//System.out.println("Queries After bobj"+queries);
		
		Document userProfile = coll.aggregate(queries).first();
		//System.out.println("UserProfile" +  userProfile);
		result.put("userid", new ObjectByteIterator(String.valueOf(profileOwnerID).getBytes()));

		//System.out.println("Result"+result);
		
		userProfile.forEach((k, v) -> {
			if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
				result.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
			}
		});

		
		
		result.put("friendcount", new ObjectByteIterator(String.valueOf(userProfile.get(KEY_FRIEND)).getBytes()));
		if (requesterID == profileOwnerID) {
			result.put("pendingcount", new ObjectByteIterator(String.valueOf(userProfile.get(KEY_PENDING)).getBytes()));
		}
		
//		System.exit(1);
		
		Set<String> StoredKeys = result.keySet();
		
		StringBuilder sb=new StringBuilder();
		
		for(String x: StoredKeys)
		{
////			String hashSetName = "Cache" + Integer.toString(profileOwnerID);
//			if(profileOwnerID == requesterID && x.equals("p"))
//			{
//				sb.append(x+ "="+result.get(x).toString());
////				NVM.hset(hashSetName, x, result.get(x).toString());
//			}
//			else if (!x.equals("p"))
//			{
//				sb.append(x+ "="+result.get(x).toString());
////				NVM.hset(hashSetName, x, result.get(x).toString());
//			}
			
			
			
//---------------Changed By Kaushal on Nov 10---------------//			
			if(x.equals("p"))
			{
				NVM.set("p_"+Integer.toString(profileOwnerID), result.get(x).toString());
			}
			else if(x.equals("f"))
			{
				NVM.set("f_"+Integer.toString(profileOwnerID), result.get(x).toString());
			}
			else
			{
				sb.append(x+"="+result.get(x).toString());
				
			}

//---------------Changed By Kaushal on Nov 10---------------//		
			
			
			sb.append("USCVILLAGE");
		}
		NVM.set(Integer.toString(profileOwnerID),sb.toString());
		
		
		
		
		}
	
		
		else
		{
//			System.out.println("Cache Hit");
			
			value = value.substring(1, value.length()-1);    
			String[] keyValuePairs = value.split("USCVILLAGE");  
				
			for(String pair : keyValuePairs)      
			{
			    String[] entry = pair.split("=");                   
			    result.put(entry[0].trim(), new ObjectByteIterator(String.valueOf(entry[1].trim()).getBytes()));         
			}
			
			
			
			
//			for(String field:RedisHsetField)
//			{
//				//System.out.println(field + " is the field");
//				try
//				{
//					byte[] tmp = NVM.hget("Cache"+Integer.toString(profileOwnerID), field).getBytes();
//					if(tmp!=null)
//					{
//						result.put(field, new ObjectByteIterator(tmp));
//					}
//				}
//				catch(Exception e)
//				{
//					
//				}
//			}
		}
		}
		
		
		else if(NvmIsUp==2 || NvmIsUp==3)
		{
			MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_USER_COLLECTION);

		
			
			List<Bson> queries = new ArrayList<Bson>();
			queries.add(new BasicDBObject("$match",
					new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
			
			//System.out.println("Queries" + queries);
			
			
			BasicDBObject obj = new BasicDBObject();
			obj.put("f", new BasicDBObject("$size", "$f"));
			obj.put("p", new BasicDBObject("$size", "$p"));
			obj.put("username", 1);
			obj.put("pw", 1);
			obj.put("fname", 1);
			obj.put("lname", 1);
			obj.put("gender", 1);
			obj.put("dob", 1);
			obj.put("jdate", 1);
			obj.put("ldate", 1);
			obj.put("address", 1);
			obj.put("email", 1);
			obj.put("tel", 1);
			BasicDBObject bobj = new BasicDBObject("$project", obj);
			queries.add(bobj);

			//System.out.println("Queries After bobj"+queries);
			
			Document userProfile = coll.aggregate(queries).first();
			//System.out.println("UserProfile" +  userProfile);
			result.put("userid", new ObjectByteIterator(String.valueOf(profileOwnerID).getBytes()));

			//System.out.println("Result"+result);
			
			userProfile.forEach((k, v) -> {
				if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
					result.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
				}
			});

			
			
			result.put("friendcount", new ObjectByteIterator(String.valueOf(userProfile.get(KEY_FRIEND)).getBytes()));
			if (requesterID == profileOwnerID) {
				result.put("pendingcount", new ObjectByteIterator(String.valueOf(userProfile.get(KEY_PENDING)).getBytes()));
			}
	
		}
		
		//System.out.println("RESULT" + result.toString());
		//System.out.println("ENd");
		
		return 0;
	}

	@Override
	public void buildIndexes(Properties props) {
		super.buildIndexes(props);
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		
		
		//NVM.flushDB();
		
		//String value=NVM.get("ListFriends_"+profileOwnerID);
		
		//---------------Changed By Kaushal on Nov 10---------------//
		if(NvmIsUp==1)
		{
		boolean callPstore = false;
		HashSet<String> values = (HashSet<String>) NVM.smembers("f_"+profileOwnerID);
		//System.out.println(values + " LRANGE RESULT");
		if(values.size()==0)
		{
			//System.out.println("Miss");
			callPstore = true;
		}
		else
		{
			int flag = 0;
			for(String i: values)
			{
				String value = NVM.get(i);
				//System.out.println("NVM GET VALUE:" + value);
				if(value==null || value.length()==0)
				{
					//System.out.println("Miss");
					callPstore = true;
					flag =1;
					break;
				}
				value = value.substring(1, value.length()-1);    
				String[] keyValuePairs = value.split("USCVILLAGE");  
				HashMap<String, ByteIterator> oneFriendsDoc = new HashMap<String, ByteIterator>();	
				for(String pair : keyValuePairs)      
				{
				    String[] entry = pair.split("=");                   
				    oneFriendsDoc.put(entry[0].trim(), new ObjectByteIterator(String.valueOf(entry[1].trim()).getBytes()));         
				}
				
				result.add(oneFriendsDoc);
				
			}
//			if(flag==0)
//			System.out.println("List friend Cache Hit for Profile Owner ID" + profileOwnerID + ": friends" + values );
			
		}
		
		
		if(callPstore)
		{
			result.removeAllElements();
			MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_USER_COLLECTION);
	
			List<Bson> list = new ArrayList<>();
			list.add(new BasicDBObject("$match",
					new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
			BasicDBList field = new BasicDBList();
			field.add("$f");
			field.add(0);
			field.add(LIST_FRIENDS);
			BasicDBObject bobj = new BasicDBObject("$project", new BasicDBObject("f", new BasicDBObject("$slice", field)));
			list.add(bobj);
			Document userProfile = coll.aggregate(list).first();
			List<String> friends = userProfile.get(KEY_FRIEND, List.class);
	
			List<Bson> queries = new ArrayList<Bson>();
			queries.add(new BasicDBObject("$match", new BasicDBObject("_id", new BasicDBObject("$in", friends))));
			BasicDBObject obj = new BasicDBObject();
			obj.put("f", new BasicDBObject("$size", "$f"));
			obj.put("username", 1);
			obj.put("pw", 1);
			obj.put("fname", 1);
			obj.put("lname", 1);
			obj.put("gender", 1);
			obj.put("dob", 1);
			obj.put("jdate", 1);
			obj.put("ldate", 1);
			obj.put("address", 1);
			obj.put("email", 1);
			obj.put("tel", 1);
			bobj = new BasicDBObject("$project", obj);
			queries.add(bobj);
			queries.add(new BasicDBObject("$limit", LIST_FRIENDS));
	
			MongoCursor<Document> friendsDocs = coll.aggregate(queries).iterator();
			
			String CONSTANT_USC_VILLAGE_INSIDE_HASHMAP="USC_VILLAGE";
			String CONSTANT_USC_MONGO_INSIDE_VECTOR="USC_MONGO_VILLAGE";
			
			
			while (friendsDocs.hasNext()) {
				StringBuilder sbtemp=new StringBuilder();
				Document doc = friendsDocs.next();
				HashMap<String, ByteIterator> val = new HashMap<String, ByteIterator>();
				val.put("userid", new ObjectByteIterator(doc.getString("_id").getBytes()));
				
				NVM.sadd("f_"+Integer.toString(profileOwnerID),doc.getString("_id") );
				sbtemp.append("userid"+CONSTANT_USC_VILLAGE_INSIDE_HASHMAP+new ObjectByteIterator(doc.getString("_id").getBytes()).toString());
	
				doc.forEach((k, v) -> {
					if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
						val.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
						sbtemp.append(k+CONSTANT_USC_VILLAGE_INSIDE_HASHMAP+new ObjectByteIterator(String.valueOf(v).getBytes()).toString());
					}
				});
				
				val.put("friendcount",
						new ObjectByteIterator(String.valueOf(doc.get(KEY_FRIEND)).getBytes()));
				
				sbtemp.append("friendcount"+CONSTANT_USC_VILLAGE_INSIDE_HASHMAP+String.valueOf(doc.get(KEY_FRIEND)));
				sbtemp.append(CONSTANT_USC_MONGO_INSIDE_VECTOR);
				NVM.set(doc.getString("_id") , sbtemp.toString());
				result.add(val);
			}
			
//			NVM.set(Integer.toString(profileOwnerID), sbtemp.toString());
			
			
			
			friendsDocs.close();
		}
		
		else
		{
//			String CONSTANT_USC_VILLAGE_INSIDE_HASHMAP="USC_VILLAGE";
//			String CONSTANT_USC_MONGO_INSIDE_VECTOR="USC_MONGO_VILLAGE";
//			
//			String hashmaps[]=value.split(CONSTANT_USC_MONGO_INSIDE_VECTOR);
//			HashMap<String,ByteIterator> current=new HashMap<>();
//			if(current.size()!=0)
//			{
//				for(String hashmap:hashmaps)
//				{	 
//					System.out.println("hashmap "+hashmap);
//				    String[] entry = hashmap.split(CONSTANT_USC_VILLAGE_INSIDE_HASHMAP);                   
//				    current.put(entry[0].trim(), new ObjectByteIterator(String.valueOf(entry[1].trim()).getBytes()));  
//					result.add(current);
//				}
//			}
				
		}
		}
		else if(NvmIsUp==2 || NvmIsUp==3)
		{
			MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
					.getCollection(MONGO_USER_COLLECTION);

			List<Bson> list = new ArrayList<>();
			list.add(new BasicDBObject("$match",
					new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
			BasicDBList field = new BasicDBList();
			field.add("$f");
			field.add(0);
			field.add(LIST_FRIENDS);
			BasicDBObject bobj = new BasicDBObject("$project", new BasicDBObject("f", new BasicDBObject("$slice", field)));
			list.add(bobj);
			Document userProfile = coll.aggregate(list).first();
			List<String> friends = userProfile.get(KEY_FRIEND, List.class);

			List<Bson> queries = new ArrayList<Bson>();
			queries.add(new BasicDBObject("$match", new BasicDBObject("_id", new BasicDBObject("$in", friends))));
			BasicDBObject obj = new BasicDBObject();
			obj.put("f", new BasicDBObject("$size", "$f"));
			obj.put("username", 1);
			obj.put("pw", 1);
			obj.put("fname", 1);
			obj.put("lname", 1);
			obj.put("gender", 1);
			obj.put("dob", 1);
			obj.put("jdate", 1);
			obj.put("ldate", 1);
			obj.put("address", 1);
			obj.put("email", 1);
			obj.put("tel", 1);
			bobj = new BasicDBObject("$project", obj);
			queries.add(bobj);
			queries.add(new BasicDBObject("$limit", LIST_FRIENDS));

			MongoCursor<Document> friendsDocs = coll.aggregate(queries).iterator();

			while (friendsDocs.hasNext()) {
				Document doc = friendsDocs.next();
				HashMap<String, ByteIterator> val = new HashMap<String, ByteIterator>();
				val.put("userid", new ObjectByteIterator(doc.getString("_id").getBytes()));
				
				

				doc.forEach((k, v) -> {
					if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
						val.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
					}
				});

				val.put("friendcount",
						new ObjectByteIterator(String.valueOf(doc.get(KEY_FRIEND)).getBytes()));
				result.add(val);
			}
			friendsDocs.close();
		}
		return 0;
	}

	public List<String> listFriends(int profileOwnerID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		return coll.find(eq("_id", String.valueOf(profileOwnerID))).first().get(KEY_FRIEND, List.class);
	}

	public List<String> listPendingFriends(int profileOwnerID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		return coll.find(eq("_id", String.valueOf(profileOwnerID))).first().get(KEY_PENDING, List.class);
	}

	boolean journaled = false;
	
	
	@Override
	public boolean init() throws DBException {
		Properties p=getProperties();
		NVM.set("HB", "ON");
//		int faileddurationtime=Integer.parseInt(p.getProperty("failedmodeduration"));
//		int normalmodetime=Integer.parseInt(p.getProperty("normalmodetime"));
		
		synchronized(this) {
			if(first_time.get()==true)
			{
				Basic b=new Basic(failedmode,null);
//				System.out.println("INIT BLOCK SYNCHR");
//				try{
//				threadsection(b,10,20);
//				System.out.println("INIT BLOCK SYNCHR FINSIHED");
//				}catch(Exception e){}
				Thread t = new Thread(b);
				t.start();
				first_time.set(false);
			}
		}
//		
//		int x=10;
//		int y=20;
		try{
		//threadsection(b,x,y);
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		System.out.println("s###init");
		
		if (getProperties().getProperty(KEY_MONGO_DB_IP) != null) {
			this.ipAddress = getProperties().getProperty(KEY_MONGO_DB_IP);
		}

		if (getProperties().getProperty("journaled") != null) {
			this.journaled = Boolean.parseBoolean(getProperties().getProperty("journaled"));
		}
		
		System.out.println("Journaled = "+journaled);

		if (journaled) {
			this.mongoClient = new MongoClient(this.ipAddress, new MongoClientOptions.Builder()
					.serverSelectionTimeout(1000).connectionsPerHost(500)
							.writeConcern(WriteConcern.JOURNALED).build());
		} else {
			this.mongoClient = new MongoClient(this.ipAddress, new MongoClientOptions.Builder()
					.serverSelectionTimeout(1000).connectionsPerHost(500)
							.writeConcern(WriteConcern.ACKNOWLEDGED).build());
		}
		return true;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		List<Bson> list = new ArrayList<>();
		list.add(new BasicDBObject("$match",
				new BasicDBObject("_id", new BasicDBObject("$eq", String.valueOf(profileOwnerID)))));
		BasicDBList field = new BasicDBList();
		field.add("$p");
		field.add(0);
		field.add(LIST_FRIENDS);
		BasicDBObject bobj = new BasicDBObject("$project", new BasicDBObject("p", new BasicDBObject("$slice", field)));
		list.add(bobj);
		Document userProfile = coll.aggregate(list).first();
		List<String> pending = userProfile.get(KEY_PENDING, List.class);

		List<Bson> queries = new ArrayList<Bson>();
		queries.add(new BasicDBObject("$match", new BasicDBObject("_id", new BasicDBObject("$in", pending))));
		BasicDBObject obj = new BasicDBObject();
		obj.put("f", new BasicDBObject("$size", "$f"));
		obj.put("username", 1);
		obj.put("pw", 1);
		obj.put("fname", 1);
		obj.put("lname", 1);
		obj.put("gender", 1);
		obj.put("dob", 1);
		obj.put("jdate", 1);
		obj.put("ldate", 1);
		obj.put("address", 1);
		obj.put("email", 1);
		obj.put("tel", 1);
		bobj = new BasicDBObject("$project", obj);
		queries.add(bobj);
		queries.add(new BasicDBObject("$limit", LIST_FRIENDS));

		MongoCursor<Document> friendsDocs = coll.aggregate(queries).iterator();

		while (friendsDocs.hasNext()) {
			Document doc = friendsDocs.next();
			HashMap<String, ByteIterator> val = new HashMap<String, ByteIterator>();
			val.put("userid", new ObjectByteIterator(doc.getString("_id").getBytes()));
			doc.forEach((k, v) -> {
				if (!KEY_FRIEND.equals(k) && !KEY_PENDING.equals(k)) {
					val.put(k, new ObjectByteIterator(String.valueOf(v).getBytes()));
				}
			});
			val.put("friendcount", new ObjectByteIterator(String.valueOf(doc.get(KEY_FRIEND)).getBytes()));
			results.add(val);
		}
		friendsDocs.close();
		return 0;
	}

	public int acceptFriendInviter(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		UpdateResult result = coll.updateOne(eq("_id", String.valueOf(inviterID)),
				new BasicDBObject("$addToSet", new Document(KEY_FRIEND, String.valueOf(inviteeID))));
		
		if(NvmIsUp==1)
		{
			NVM.sadd("f_"+Integer.toString(inviterID), Integer.toString(inviteeID));
		}
		else
		{
			TSA.sadd(Integer.toString(inviterID), "f_add_"+Integer.toString(inviteeID));
		}
	
		
		return 0;
	}

	/**
	 * Update user's friends and pending friends. If friends / pending friends
	 * are specified, add/remove friends / pending friends are ignored.
	 * 
	 * @param friends
	 * @param pendingFriends
	 * @param addFriends
	 * @param removeFriends
	 * @param addPendingFriends
	 * @param removePendingFriends
	 * @return
	 */
	public int updateUserDocument(String userId, Set<String> friends, Set<String> pendingFriends,
			Set<String> addFriends, Set<String> removeFriends, Set<String> addPendingFriends,
			Set<String> removePendingFriends) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		Document addToSet = new Document();
		Document pull = new Document();
		Document set = new Document();

		if (friends != null) {
			set.put(KEY_FRIEND, friends);
		} else {
			if (!isEmpty(addFriends)) {
				addToSet.put(KEY_FRIEND, new BasicDBObject("$each", addFriends));
			}
			if (!isEmpty(removeFriends)) {
				pull.put(KEY_FRIEND, new BasicDBObject("$in", removeFriends));
			}
		}

		if (pendingFriends != null) {
			set.put(KEY_PENDING, pendingFriends);
		} else {
			if (!isEmpty(addPendingFriends)) {
				addToSet.put(KEY_PENDING, new BasicDBObject("$each", addPendingFriends));
			}
			if (!isEmpty(removePendingFriends)) {
				pull.put(KEY_PENDING, new BasicDBObject("$in", removePendingFriends));
			}
		}

		BasicDBObject upsert = new BasicDBObject();
		BasicDBObject remove = new BasicDBObject();

		if (!set.isEmpty()) {
			upsert.put("$set", set);
		}

		if (!addToSet.isEmpty()) {
			upsert.put("$addToSet", addToSet);
		}

		if (!pull.isEmpty()) {
			remove.put("$pull", pull);
		}

		List<WriteModel<Document>> list = new ArrayList<>();

		if (!upsert.isEmpty()) {
			list.add(new UpdateOneModel<Document>(eq("_id", userId), upsert));
		}
		if (!pull.isEmpty()) {
			list.add(new UpdateOneModel<Document>(eq("_id", userId), remove));
		}
		// then pull from the set
		if (!list.isEmpty()) {
			log.debug("Bulk update on user " + userId + " " + upsert.toJson() + " " + remove.toJson());
			coll.bulkWrite(list);
		} else {
			log.debug("Nothing to recover for user " + userId);
		}

		return 0;
	}

	private boolean isEmpty(Collection<?> coll) {
		return coll == null || coll.isEmpty();
	}

	public int acceptFriendInvitee(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		BasicDBObject inviteeUpdate = new BasicDBObject();
		inviteeUpdate.put("$addToSet", new Document(KEY_FRIEND, String.valueOf(inviterID)));
		inviteeUpdate.put("$pull", new Document(KEY_PENDING, String.valueOf(inviterID)));
		coll.updateOne(eq("_id", String.valueOf(inviteeID)), inviteeUpdate);
		
		//---------------Changed By Kaushal on Nov 10---------------//
		if(NvmIsUp==1)
		{
			NVM.sadd("f_"+Integer.toString(inviteeID), Integer.toString(inviterID));
			NVM.srem("p_"+Integer.toString(inviteeID),Integer.toString(inviterID));
		}
		else if(NvmIsUp==2)
		{
			TSA.sadd(Integer.toString(inviteeID), "f_add_"+Integer.toString(inviterID));
			TSA.sadd(Integer.toString(inviteeID),"p_remove_"+Integer.toString(inviterID));
		}
		
		//---------------Changed By Kaushal on Nov 10---------------//
		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		acceptFriendInviter(inviterID, inviteeID);
		acceptFriendInvitee(inviterID, inviteeID);
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		coll.updateOne(eq("_id", String.valueOf(inviteeID)),
				new BasicDBObject("$pull", new Document(KEY_PENDING, String.valueOf(inviterID))));
		
		//---------------Changed By Kaushal on Nov 10---------------//
		if(NvmIsUp==1)
		{
			NVM.srem("p_"+Integer.toString(inviteeID), Integer.toString(inviterID));
		}
		else if(NvmIsUp==2)
		{
			TSA.sadd(Integer.toString(inviteeID), "p_remove_"+Integer.toString(inviterID));
		}
		//---------------Changed By Kaushal on Nov 10---------------//
		
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		coll.updateOne(eq("_id", String.valueOf(inviteeID)),
				new BasicDBObject("$addToSet", new Document(KEY_PENDING, String.valueOf(inviterID))));
		
		//---------------Changed By Kaushal on Nov 10---------------//
		if(NvmIsUp==1)
		{
			NVM.sadd("p_"+Integer.toString(inviteeID), Integer.toString(inviterID));
		}
		//---------------Changed By Kaushal on Nov 10---------------//
		else if(NvmIsUp==2)
		{
			TSA.sadd(Integer.toString(inviteeID), "p_add_"+Integer.toString(inviterID));
		}
		
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
		// TODO Auto-generated method stub
		return 0;
	}

	public int thawFriendInviter(int friendid1, int friendid2) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);

		coll.updateOne(eq("_id", String.valueOf(friendid1)),
				new BasicDBObject("$pull", new Document(KEY_FRIEND, String.valueOf(friendid2))));
		
		//---------------Changed By Kaushal on Nov 10---------------//
		if(NvmIsUp==1)
		{
			NVM.srem("f_"+Integer.toString(friendid1), Integer.toString(friendid2));
		}
		else if(NvmIsUp==2)
		{
			TSA.sadd(Integer.toString(friendid1), "f_remove_"+Integer.toString(friendid2));
		}
		
		//---------------Changed By Kaushal on Nov 10---------------//
		
		
		return 0;
	}

	public int thawFriendInvitee(int friendid1, int friendid2) {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		coll.updateOne(eq("_id", String.valueOf(friendid2)),
				new BasicDBObject("$pull", new Document(KEY_FRIEND, String.valueOf(friendid1))));
		
		//---------------Changed By Kaushal on Nov 10---------------//
		if(NvmIsUp==1)
		{
			NVM.srem("f_"+Integer.toString(friendid2),Integer.toString(friendid1));
		}
		else if(NvmIsUp==2)
		{
			TSA.sadd(Integer.toString(friendid2), "f_remove_"+Integer.toString(friendid1));
		}
		
		//---------------Changed By Kaushal on Nov 10---------------//
		
		
		return 0;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		thawFriendInviter(friendid1, friendid2);
		thawFriendInvitee(friendid1, friendid2);
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
				.getCollection(MONGO_USER_COLLECTION);
		HashMap<String, String> stats = new HashMap<>();
		System.out.println("initialized users " + coll.count());
		try{
		stats.put("usercount", String.valueOf(coll.count()));
		stats.put("resourcesperuser", "0");
		
		stats.put("avgfriendsperuser", String.valueOf(coll.find().first().get(KEY_FRIEND, List.class).size()));
		
		stats.put("avgpendingperuser", String.valueOf(coll.find().first().get(KEY_PENDING, List.class).size()));
		}catch(Exception e){}
		return stats;
	}
	
	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		createFriendship.compareAndSet(false, true);
		return 0;
	}

	@Override
	public void createSchema(Properties props) {
		mongoClient.dropDatabase(MONGO_DB_NAME);
		MongoDatabase db = mongoClient.getDatabase(MONGO_DB_NAME);
		db.createCollection(MONGO_USER_COLLECTION);
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		System.out.println("###clean");
		if (!warmup && createFriendship.get() == true) {
			if (getProperties().getProperty("reconaction") != null &&
					getProperties().getProperty("reconaction").equals("load"))				
				bulkWriteFriends();
		}
		mongoClient.close();
		super.cleanup(warmup);
	}

	public void bulkWriteFriends() {
		try {
			loadFriends.acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			if (friendLoad.get() == false) {
				int numUsers = Integer.parseInt(getProperties().getProperty("usercount"));
				int numFriendsPerUser = Integer.parseInt(getProperties().getProperty("friendcountperuser"));
//				int numFriendsPerUser = 100;
				
				MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
						.getCollection(MONGO_USER_COLLECTION);
				List<WriteModel<Document>> list = new ArrayList<>();
				
				int k = 1000;
				int loop = numUsers / k;
				int curr = 0;
				
				while (curr < loop) {				
					for (int i = k * curr; i < k*(curr+1); i++) {
						Set<String> friends = new HashSet<>();
						for (int j = 1; j <= numFriendsPerUser / 2; j++) {
							int id = i+j;
							if (id >= numUsers) id -= numUsers;
							friends.add(String.valueOf(id));
							
							id = i-j;
							if (id < 0) id += numUsers;
							friends.add(String.valueOf(id));
						}		
						
						list.add(new UpdateOneModel<Document>(eq("_id", String.valueOf(i)),
								new BasicDBObject("$set", new Document(KEY_FRIEND, friends))));
					}
					
					coll.bulkWrite(list);
					list.clear();
					
					curr++;
					System.out.println("Loaded "+k*curr+" users.");
				}
				
				friendLoad.set(true);
			}
		} catch (NumberFormatException e) {
			
		}
		
		loadFriends.release();
	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		System.out.println("queryPendingFriendshipIds");
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		System.out.println("queryConfirmedFriendshipIds");
		return 0;
	}
	
	 public String getCurrentState(int id) {
	    MongoCollection<Document> coll = this.mongoClient.getDatabase(MONGO_DB_NAME)
	        .getCollection(MONGO_USER_COLLECTION);
	    Document userProfile = coll.find(eq("_id", String.valueOf(id))).first();
	    List<String> fs = userProfile.get(KEY_FRIEND, List.class);
	    List<String> ps = userProfile.get(KEY_PENDING, List.class);
	    StringBuilder res = new StringBuilder();
	    for (String f: fs)
	      res.append(f).append(",");
	    
	    if (res.length() > 0 && res.charAt(res.length()-1) == ',')
	      res.setCharAt(res.length()-1, ';');
	    else
	      res.append(";");
	    
	    for (String p: ps)
	      res.append(p).append(",");
	    
	    if (res.length() > 0 && res.charAt(res.length()-1) == ',')
	      res.deleteCharAt(res.length()-1);
	    return res.toString();
	  }
}


//class Basic2 implements Runnable
//{	
//	AtomicBoolean failedmode;
//	Jedis NVM;
//	Jedis TSA;
//	
//	public Basic(AtomicBoolean failedmode) {
//		 NVM=new Jedis("localhost",6380);
//		 TSA=new Jedis("localhost",6379);
//	}
//
//	@Override
//	public void run() {
//		// TODO Auto-generated method stub
//		
//		TSA_to_NVM_Transfer();
//		
//	}
//	
//	
//	public void TSA_to_NVM_Transfer()
//	{
//		System.out.println("STARTED"+System.nanoTime());
//		for(String x:TSA.keys("*"))
//		{
//			HashSet<String> current=(HashSet<String>) TSA.smembers(x);
//			for(String command:current)
//			{
//				String listtocheck=command.substring(0,command.indexOf("_"));
//				String action=command.substring(command.indexOf("_")+1,command.lastIndexOf("_"));
//				String value=command.substring(command.lastIndexOf("_")+1);
//				String exists_val=NVM.get(x);
//				if(exists_val!=null)
//				{
//					if(action.equals("add"))
//					{
//						NVM.sadd(listtocheck+"_"+x, value);
//					}
//					else if(action.equals("remove"))
//					{
//						NVM.srem(listtocheck+"_"+x, value);
//					}
//				}
//			}
//		}
//		System.out.println("FINSIHED"+System.nanoTime());
//		TSA.flushDB();
//	}
//	
//	
//}



class Basic implements Runnable
{
	AtomicBoolean failedmode;
	Jedis NVM=new Jedis("localhost",6380);
	Jedis TSA=new Jedis("localhost",6379);
	List<String> currentlist;
	public Basic(AtomicBoolean failedmode,List<String> currentlist) {
		this.failedmode = failedmode;
		this.currentlist=currentlist;
	}
	public void TSA_to_NVM_Transfer()
	{	
		System.out.println("STARTED"+System.nanoTime());
		for(String x:currentlist)
		{
			HashSet<String> current=(HashSet<String>) TSA.smembers(x);
			for(String command:current)
			{
				if(command!=null && command.length()!=0 && command.contains("_"))
				{
					String listtocheck=command.substring(0,command.indexOf("_"));
					String action=command.substring(command.indexOf("_")+1,command.lastIndexOf("_"));
					String value=command.substring(command.lastIndexOf("_")+1);
					String exists_val=NVM.get(x);
					if(exists_val!=null)
					{
						if(action.equals("add"))
						{
							NVM.sadd(listtocheck+"_"+x, value);
						}
						else if(action.equals("remove"))
						{
							NVM.srem(listtocheck+"_"+x, value);
						}
					}
				}
			}
		}
		System.out.println("FINSIHED"+System.nanoTime());
//		TSA.flushDB();
	}
	public static boolean call_ar_workers(AtomicBoolean failedmode,ArrayList<String> fulllist,int size)
	{
		for(int i=0;i<10;i++)
		{
			new Thread(new Basic(failedmode,fulllist.subList((size/100)*i,(size/100)*(i+1)))).start();
		}
		return true;
	}

	public void threadsection(int x,int y) throws InterruptedException
	{
		if(failedmode.get()==false)
		{
			failedmode.set(true);
			System.out.println("YOU CAME HERE BEGIN THREAD SECTION");
			BackgroundThread BGG=new BackgroundThread(failedmode);
			Thread bgthread=new Thread(BGG);
			bgthread.start();
			Thread.sleep(x*1000);
			NVM.set("HB", "OFF");
			//MongoBGClient.NvmIsUp=2;
			System.out.println("NVM IS DOWN.");
			Thread.sleep(y*1000);
			//MongoBGClient.NvmIsUp=3;
			NVM.set("HB", "ON");
			
			
		//	NVM.set("HB", "ON");
			//TSA_to_NVM_Transfer();
		//	System.out.println("TRANSFER FINISHED");
		//	MongoBGClient.NvmIsUp=1;
		//	System.out.println("NORMAL MODE AGAIN");
		}
		else
		{
			System.out.println("else section thread");
		}
	}
	
	@Override
	public void run() {
		try {
			if(failedmode.get()==false)
			{
				threadsection(10, 20);
			}
			else if(failedmode.get()==true)
			{
				TSA_to_NVM_Transfer();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
class BackgroundThread implements Runnable
{
	Jedis NVM=new Jedis("localhost",6380);
	Jedis TSA=new Jedis("localhost",6379);
	AtomicBoolean failedmode;
	public BackgroundThread(AtomicBoolean failedmode)
	{
		this.failedmode=failedmode;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		 try
	        {
	        	while(true) {
	        		String HBVal=NVM.get("HB");
	        		  //System.out.println("Val" + val + "NVM"+isNVM);
	        		if(HBVal.equals("ON") && MongoBGClient.NvmIsUp==2) {
	        			
	        			//System.out.println(MongoBGClient.NvmIsUp + "Switched to Recovery");
	        			MongoBGClient.NvmIsUp=3;
	        			HashSet<String> getallkeys=(HashSet<String>) TSA.keys("*");
	        			ArrayList<String> fulllist=new ArrayList<>(getallkeys);
	        			int size=fulllist.size();
	        			
	        			MongoBGClient.isRecovery=mongoDB.Basic.call_ar_workers(failedmode,fulllist,size);
	        			//MongoBGClient.NvmIsUp=1;
	        			
	        		}
	        		else if(HBVal.equals("ON") && MongoBGClient.isRecovery==true) {
	        			//System.out.println(MongoBGClient.NvmIsUp + "Switched to Normal after recovery complete");
	        			MongoBGClient.NvmIsUp=1;
	        		}
	        		else if (HBVal.equals("OFF")) {
	        			//isFailure started - setup
	        			//System.out.println("In failed mode");
	        			MongoBGClient.NvmIsUp=2;
	        		}
	        	
	        	}
	 
	        }
	        catch (Exception e)
	        {
	            // Throwing an exception
	            System.out.println ("Exception is caught");
	        }
	}
	
}