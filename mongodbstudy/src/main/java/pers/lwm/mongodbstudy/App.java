package pers.lwm.mongodbstudy;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

/**
 * java使用mongodb
 * 
 * mongo-java-driver 这是 个组合包
 * 
 * mongo-driver 这是单个包 两者都可以使用
 *
 * 学习资料
 * http://mongodb.github.io/mongo-java-driver/3.0/driver/getting-started/quick-
 * tour/
 */
public class App {

	// 不带密码链接
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient("10.10.17.8", 27017);
		MongoDatabase mongoDatabase = mongoClient.getDatabase("databaseName");

		System.out.println("Connect to database successfully");
	}

	// 带密码的链接
	public static void main_2(String[] args) {
		try {
			// 连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
			// ServerAddress()两个参数分别为 服务器地址 和 端口
			ServerAddress serverAddress = new ServerAddress("10.10.17.8", 27017);
			List<ServerAddress> addrs = new ArrayList<ServerAddress>();
			addrs.add(serverAddress);

			// MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
			MongoCredential credential = MongoCredential.createScramSha1Credential("username", "databaseName",
					"password".toCharArray());
			List<MongoCredential> credentials = new ArrayList<MongoCredential>();
			credentials.add(credential);

			// 通过连接认证获取MongoDB连接
			MongoClient mongoClient = new MongoClient(addrs, credentials);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("databaseName");
			System.out.println("Connect to database successfully");
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 创建集合
	public static void main_3(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			mongoDatabase.createCollection("test");

			System.out.println("集合创建成功");

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 获取集合
	public static void main_4(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			// 统计总数
			System.out.println(collection.count());

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 插入一个文档
	public static void main_5(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			// 封装文档
			Document doc = new Document("name", "MongoDB").append("type", "database").append("count", 1).append("info",
					new Document("x", 203).append("y", 102));

			collection.insertOne(doc);

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 插入多个文档
	public static void main_6(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			// 封装文档
			List<Document> documents = new ArrayList<Document>();
			for (int i = 0; i < 100; i++) {
				documents.add(new Document("i", i));
			}

			collection.insertMany(documents);

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 查询第一个文档
	public static void main_7(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			Document myDoc = collection.find().first();
			System.out.println(myDoc.toJson());

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 查询所有文档
	public static void main_8(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			MongoCursor<Document> cursor = collection.find().iterator();
			try {
				while (cursor.hasNext()) {
					System.out.println(cursor.next().toJson());
				}
			} finally {
				cursor.close();
			}

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 查询一个文档
	public static void main_9(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			// 使用eq时 import static com.mongodb.client.model.Filters.*;
			Document myDoc = collection.find(eq("i", 71)).first();
			// 返回固定的列
			// collection.find().projection(fields(include("x",
			// "y"),exclude("a","b")))
			System.out.println(myDoc.toJson());

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 获取一个集合
	public static void main_10(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			// now use a range query to get a larger subset
			Block<Document> printBlock = new Block<Document>() {
				@Override
				public void apply(final Document document) {
					System.out.println(document.toJson());
				}
			};
			// 条件查询
			collection.find(gt("i", 50)).forEach(printBlock);
			// and 查询
			// collection.find(and(gt("i", 50), lte("i",100))).forEach(printBlock);

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}

	// 更新文档
	public static void main_11(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");
			
			//更新一个
			collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)));
			
			//更新多个
			UpdateResult updateResult = collection.updateMany(lt("i", 100),
			          new Document("$inc", new Document("i", 100)));
			System.out.println(updateResult.getModifiedCount());

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
	//删除文档
	public static void main_12(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			// 删除一个
			collection.deleteOne(eq("i", 110));

			// 删除多个
			DeleteResult deleteResult = collection.deleteMany(gte("i", 100));
			System.out.println(deleteResult.getDeletedCount());

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
	
	/**
	//批量操作
	public static void main_13(String args[]) {
		try {
			// 连接到 mongodb 服务
			MongoClient mongoClient = new MongoClient("localhost", 27017);

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase("mycol");

			// 获取集合
			MongoCollection<Document> collection = mongoDatabase.getCollection("test");

			
			// 2. Ordered bulk operation - order is guarenteed
			collection.bulkWrite(
			  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
			                new InsertOneModel<>(new Document("_id", 5)),
			                new InsertOneModel<>(new Document("_id", 6)),
			                new UpdateOneModel<>(new Document("_id", 1),
			                                     new Document("$set", new Document("x", 2))),
			                new DeleteOneModel<>(new Document("_id", 2)),
			                new ReplaceOneModel<>(new Document("_id", 3),
			                                      new Document("_id", 3).append("x", 4))));


			 // 2. Unordered bulk operation - no guarantee of order of operation
			collection.bulkWrite(
			  Arrays.asList(new InsertOneModel<>(new Document("_id", 4)),
			                new InsertOneModel<>(new Document("_id", 5)),
			                new InsertOneModel<>(new Document("_id", 6)),
			                new UpdateOneModel<>(new Document("_id", 1),
			                                     new Document("$set", new Document("x", 2))),
			                new DeleteOneModel<>(new Document("_id", 2)),
			                new ReplaceOneModel<>(new Document("_id", 3),
			                                      new Document("_id", 3).append("x", 4))),
			  new BulkWriteOptions().ordered(false));
			

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
		}
	}
	**/
}
