package LDA;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.Timestamp;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.sql.columnar.DATE;
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

public class SetLDA {
	
	static HashMap<Integer, Integer> appTagMap;
	
	public static void saveHashMap(String path) throws IOException
	{
		ObjectOutputStream objout = new ObjectOutputStream(new FileOutputStream(path));		
		objout.writeObject(appTagMap);
		objout.close();	
	}
	
	public static int setHashMap() throws IOException
	{
		// Set up HashMap
		appTagMap = new HashMap<Integer, Integer>();
	    String vocabPath = "data/data20150921/package_id_to_cate.log";
	    File vocabFile = new File(vocabPath);
	    BufferedReader in = new BufferedReader(new FileReader(vocabFile));
	    String buf;
	    int max = 0;
	    while(in.ready())
	    {
	    	buf = in.readLine();
	    	String[] appTagPair = buf.split(",");
	    	int appId = Integer.parseInt(appTagPair[0].trim());
	    	int cateId = Integer.parseInt(appTagPair[3].trim());
	    	if(cateId > max)
	    		max = cateId;
	    	appTagMap.put(appId, cateId);
	    }
	    in.close();

		// Save the number of words
		ObjectOutputStream maxOut = new ObjectOutputStream(new FileOutputStream("data/wordNumber.wrdnum"));
	    maxOut.writeInt(max);
	    maxOut.close();
		return max;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException 
	{
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();

		SparkConf conf = new SparkConf().setAppName("SetLDAModel");
		conf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		final int max = setHashMap();

		// Load and parse the data
		// Index with dev ID
		int passageLength = 70;
		String path = "data/data20150921/cleaned_data_length_"+passageLength+".log";
		JavaRDD<String> data = sc.textFile(path);
		JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(data.map(
			new Function<String, Tuple2<Long, Vector>>() 
			{
				public Tuple2<Long, Vector> call(String s) throws IOException 
				{					
					//get dev ID
					String[] splittedData = s.split("\t");
					long devId = Long.parseLong(splittedData[0]);
					
					//get app array
					String[] sarray = splittedData[1].split(",");
					int[] apps = new int[sarray.length];
					for (int i = 0; i < sarray.length; i++)
					{
						apps[i] = Integer.parseInt(sarray[i]);
					}
					
					//count for apps
					double[] appCount = new double[max + 1];
					for (int i = 0; i < appCount.length; i++)
					{
						appCount[i] = 0;
					}
					int cateId;
					for (int i = 0; i < apps.length; i++) 
					{
						cateId = appTagMap.getOrDefault(apps[i], 0);
						if (cateId != 0)
						{
							appCount[cateId] = appCount[cateId] + 1;
						}	
					}
					Vector appCounts = Vectors.dense(appCount);

					Tuple2<Long, Vector> devAppPair = new Tuple2<Long, Vector>(devId, appCounts);
					return devAppPair;
				}
			}
		));
		corpus.cache();
		System.out.println("Parsing and Indexing finished.");

        // Set the parameters
        int topicNum = 15;
        int maxIterations = 100;

		// Cluster the documents into n topics using LDA
		LDA lda = new LDA().setK(topicNum).setMaxIterations(maxIterations);
		DistributedLDAModel ldaModel = (DistributedLDAModel)lda.run(corpus);
		double alpha = lda.getAlpha();
		double beta = lda.getBeta();
	    
		// Save LDA Model
		MyLDAModel myLdaModel = new MyLDAModel(alpha, ldaModel.topicsMatrix(), ldaModel.k(), ldaModel.vocabSize());
		myLdaModel.save("data/ldaModel.ldamod");
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
		myLdaModel.saveTopicTermMatrix("verification/" + timeStamp +"_topic_" + topicNum + "_alpha_" + alpha + "_beta_" + beta + "_iter_" + maxIterations + "_length_" +passageLength+ ".ttmatr");
        saveHashMap("data/appTagMap.hashmap");

        stopwatch.stop();
        long timeTaken = stopwatch.getTime();
		
		System.out.println("Setup LDA Model successfully. Time: " + timeTaken + "ms");
		
		sc.stop();
		sc.close();
	}
}
