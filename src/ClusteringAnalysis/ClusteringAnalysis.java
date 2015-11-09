package ClusteringAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.json.JSONArray;  
import org.json.JSONException;    
import org.json.JSONObject; 

import scala.Tuple2;
import LDA.MyLDAModel;

public class ClusteringAnalysis {
	
	public static KMeansModel setKMeansModel() throws IOException, ClassNotFoundException
	{
		MyLDAModel ldaModel;
		HashMap<Integer, Integer> appTagMap;
		ObjectInputStream objin = new ObjectInputStream(new FileInputStream("data/ldaModel.ldamod"));
		ldaModel = (MyLDAModel)objin.readObject();
		objin.close();
		ObjectInputStream mapin = new ObjectInputStream(new FileInputStream("data/appTagMap.hashmap"));
		appTagMap = (HashMap<Integer, Integer>)mapin.readObject();
		mapin.close();
		ObjectInputStream maxin = new ObjectInputStream(new FileInputStream("data/data/wordNumber.wrdnum"));
		int max = maxin.readInt();
		maxin.close();
		
		SparkConf conf = new SparkConf().setAppName("KMeansClustering");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = "data/realData/20141201/part-r-00000";
		JavaRDD<String> data = sc.textFile(path);
		
		JavaRDD<Vector> parsedData = data.map(
			new Function<String, Vector>()
			{
				public Vector call(String s) 
				{
					//get dev ID
					String[] splittedData = s.split("\t");
					//get app array
					String[] sarray = splittedData[1].split(",");
					int[] apps = new int[sarray.length];
					for (int i = 0; i < sarray.length; i++)
						apps[i] = Integer.parseInt(sarray[i]);
					//count for apps
					double[] appCount = new double[max + 1];
					for (int i = 0; i < appCount.length; i++)
						appCount[i] = 0;
					int cateId;
					for (int i = 0; i < apps.length; i++) 
					{
						cateId = appTagMap.getOrDefault(apps[i], 0);
						if (cateId != 0)
							appCount[cateId] = appCount[cateId] + 1;
					}
					Vector usr = Vectors.dense(appCount);
//					System.out.println(usr);
				    double[] result = new double[ldaModel.k()];				    
				    result = ldaModel.predict(usr);
//				    for(int i=0; i<result.length; i++)
//				    {
//				    	System.out.print(result[i]);
//				    }
//				    System.out.println();
				    Vector topics = Vectors.dense(result);
				    return topics;
				}
			}
		);
		parsedData.cache();
		
		// Cluster the data into two classes using KMeans
	    int numClusters = 5;
	    int numIterations = 50;
	    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
	    
	    // Save model
	    // clusters.save(sc.sc(), "data/KMeansModel.kmod");
	    
	    sc.close();
	    return clusters;
	}

	public static int[][] sortForTermsN(MyLDAModel ldaModel, int getTopNum)
	{
		int[][] result = new int[ldaModel.k()][getTopNum];
		double[][] topicTermsMatrix = ldaModel.topicsMatrix();
		double[][] changedMatrix = new double[ldaModel.k()][ldaModel.vocabSize()];
		double[][] sortedMatrix = new double[ldaModel.k()][ldaModel.vocabSize()];
		
		for(int i=0; i<ldaModel.k(); i++)
		{
			for(int j=0; j<ldaModel.vocabSize(); j++)
			{
				changedMatrix[i][j] = topicTermsMatrix[j][i];
			}
		}
		for(int i=0; i<ldaModel.k(); i++)
		{
			for(int j=0; j<ldaModel.vocabSize(); j++)
			{
				sortedMatrix[i][j] = topicTermsMatrix[j][i];
			}
		}
		for(int i=0; i<ldaModel.k(); i++)
		{
			Arrays.sort(sortedMatrix[i]);
		}
		
		for(int i=0; i<ldaModel.k(); i++)
		{
			for(int j=0; j<getTopNum; j++)
			{
				for(int k=0; k<ldaModel.vocabSize(); k++)
				{
					if(changedMatrix[i][k] == sortedMatrix[i][ldaModel.vocabSize() - 1 - j])
						result[i][j] = k;
				}
			}
		}
				
		return result;
	}
	
	public static String[][] getTopicCateName(int[][] sortResult, MyLDAModel ldaModel, int getTopNum) throws IOException
	{
		String[][] result = new String[ldaModel.k()][getTopNum];
		String[] cateNames = new String[ldaModel.vocabSize()];
		BufferedReader in = new BufferedReader(new FileReader("data/topicCheck/vocab.txt"));
		int i = 0;
		while(in.ready())
		{
			cateNames[i] = in.readLine();
			i += 1;
		}
		in.close();		
		
		for(i=0; i<ldaModel.k(); i++)
		{
			for(int j=0; j<getTopNum; j++)
			{
				result[i][j] = cateNames[sortResult[i][j]];
			}
		}		
		return result;
	}
	
	public static double[][] getTopicCateNum(int[][] sortResult, MyLDAModel ldaModel, int getTopNum)
	{
		double[][] result = new double[ldaModel.k()][getTopNum];
		double[][] topicTermsMatrix = ldaModel.topicsMatrix();
		for(int i=0; i<ldaModel.k(); i++)
		{
			for(int j=0; j<getTopNum; j++)
			{
				result[i][j] = topicTermsMatrix[ sortResult[i][j] ][i];
			}
		}
		return result;
	}
	
	public static void generateJson(MyLDAModel ldaModel, KMeansModel clusters, 
									   HashMap<Integer, Accumulator<Integer>> crowdCountMap,
									   String topicCateName[][], double topicCateNum[][]) throws JSONException, IOException
	{
		JSONObject jsonObject = new JSONObject();		
		JSONArray jsonCrowdArr = new JSONArray();
		jsonObject.put("name", "用户头像");	
		jsonObject.put("children", jsonCrowdArr);
		for(int i=0; i<clusters.k(); i++)
		{
			JSONObject jsonCrowd = new JSONObject();
			JSONArray jsonTopicArr = new JSONArray();
			jsonCrowd.put("name", "人群" + i);
			jsonCrowd.put("size", crowdCountMap.get(i).value());
			jsonCrowd.put("children", jsonTopicArr);	
			jsonCrowdArr.put(jsonCrowd);
			for(int j=0; j<5; j++)
			{
				JSONObject jsonTopic = new JSONObject();
				jsonTopic.put("name", "topic"+(j+1));
				Vector[] centroids = clusters.clusterCenters();
				JSONArray jsonTagArr = new JSONArray();
				jsonTopic.put("children", jsonTagArr);
				jsonTopicArr.put(jsonTopic);
				for(int k=0; k<5; k++)
				{
					JSONObject jsonTag = new JSONObject();
					jsonTag.put("index", j);
					jsonTag.put("size", Math.exp((centroids[i].apply(j))) * crowdCountMap.get(i).value() * topicCateNum[j][k]);
					jsonTag.put("name", topicCateName[j][k]);
					jsonTagArr.put(jsonTag);
				}				
			}							
		}		
		
		FileOutputStream fout = new FileOutputStream(new File("data/json/data_test.json"));
		fout.write(jsonObject.toString().getBytes());
		fout.close();
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, JSONException {
		
		// Load Model
		// KMeansModel clusters = KMeansModel.load(sc.sc(), "data/KMeansModel.kmod");
		KMeansModel clusters = setKMeansModel();
		
		MyLDAModel ldaModel;
		HashMap<Integer, Integer> appTagMap;
		ObjectInputStream objin = new ObjectInputStream(new FileInputStream("data/ldaModel.ldamod"));
		ldaModel = (MyLDAModel)objin.readObject();
		objin.close();
		ObjectInputStream mapin = new ObjectInputStream(new FileInputStream("data/appTagMap.hashmap"));
		appTagMap = (HashMap<Integer, Integer>)mapin.readObject();
		mapin.close();
		ObjectInputStream maxin = new ObjectInputStream(new FileInputStream("data/numMax"));
		int max = maxin.readInt();
		maxin.close();
		
	    SparkConf conf = new SparkConf().setAppName("KMeansClustering");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		String path = "data/realData/20141201/part-r-00000";
		JavaRDD<String> data = sc.textFile(path);
	    
		JavaPairRDD<Long, Integer> result = JavaPairRDD.fromJavaRDD(data.map(
			new Function<String, Tuple2<Long, Integer>>()
			{
				public Tuple2<Long, Integer> call(String s)
				{
					//get dev ID
					String[] splittedData = s.split("\t");
					long devId = Long.parseLong(splittedData[0]);
					//get app array
					String[] sarray = splittedData[1].split(",");
					int[] apps = new int[sarray.length];
					for (int i = 0; i < sarray.length; i++)
						apps[i] = Integer.parseInt(sarray[i]);
					//count for apps
					double[] appCount = new double[max + 1];
					for (int i = 0; i < appCount.length; i++)
						appCount[i] = 0;
					int cateId;
					for (int i = 0; i < apps.length; i++) 
					{
						cateId = appTagMap.getOrDefault(apps[i], 0);
						if (cateId != 0)
							appCount[cateId] = appCount[cateId] + 1;
					}
					Vector usr = Vectors.dense(appCount);
				    double[] result = new double[ldaModel.numTopics];				    
				    result = ldaModel.predict(usr);
				    Vector topics = Vectors.dense(result);
				    int centroid = clusters.predict(topics);
				    Tuple2<Long, Integer> idCenterPair = new Tuple2<Long, Integer>(devId, centroid);
				    return idCenterPair;
				}
			}
		));	

//		// Sysout for debug
//		Vector[] a = clusters.clusterCenters();
//		for(int i=0; i<clusters.k(); i++)
//		{			
//			System.out.println(a[i]);
//		}		
//		result.foreach(
//			new VoidFunction<Tuple2<Long, Integer>>()
//			{
//				public void call(Tuple2<Long, Integer> tuple)
//				{
//					System.out.print("ID: " + tuple._1 + " ");
//					System.out.println(tuple._2);
//				}
//			}
//		);
		
		// Count for each crowd
		HashMap<Integer, Accumulator<Integer>> crowdCountMap =  new HashMap<Integer, Accumulator<Integer>>();
		for(int i=0; i<clusters.k(); i++)
		{
			Accumulator<Integer> crowdCount = sc.accumulator(0);
			crowdCountMap.put(i, crowdCount);
		}
		Accumulator<Integer> crowdCountTotal = sc.accumulator(0);
		result.foreach(
			new VoidFunction<Tuple2<Long, Integer>>()
			{
				public void call(Tuple2<Long, Integer> tuple)
				{
					crowdCountTotal.add(1);
					crowdCountMap.get(tuple._2()).add(1);
				}
			}
		);
		
//		// Sysout for debug
//		System.out.println("Total: " + crowdCountTotal.value());
//		
//		for(int i=0; i<clusters.k(); i++)
//		{
//			System.out.println("Crowd"+i+": "+crowdCountMap.get(i).value());
//		}
		
		// Generate JSON
		int[][] sortResult = sortForTermsN(ldaModel, 5);
		String[][] topicCateName = getTopicCateName(sortResult, ldaModel, 5);
		double[][] topicCateNum = getTopicCateNum(sortResult, ldaModel, 5);			
		generateJson(ldaModel, clusters, crowdCountMap, topicCateName, topicCateNum);
		System.out.println("clustering successfully and JSON has been generated.");
		
		sc.close();
	}

}
