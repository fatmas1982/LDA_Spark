package Utility;

import java.io.*;
import java.util.HashMap;

/**
 * Created by liyan on 9/16/15.
 */
public class DataPreprocess{

    static HashMap<Integer, Integer> appTagMap;

    public static int setHashMap() throws IOException {
        // Set up HashMap
        appTagMap = new HashMap<Integer, Integer>();
        String vocabPath = "data/applog/pkg_type_id.log";
        File vocabFile = new File(vocabPath);
        BufferedReader in = new BufferedReader(new FileReader(vocabFile));
        String buf;
        int max = 0;
        while(in.ready())
        {
            buf = in.readLine();
            String[] appTagPair = buf.split("\t");
            int appId = Integer.parseInt(appTagPair[0]);
            int cateId = Integer.parseInt(appTagPair[1]);
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

    public static void main(String[] args) throws IOException {
        String date = "20141203";
        BufferedReader in = new BufferedReader(new FileReader(new File("data/test/cleanedData"+date+".dat")));
//        BufferedReader in = new BufferedReader(new FileReader(new File("data/test/cleanedDataBigTest.dat")));
        String buf;
        FileOutputStream fout = new FileOutputStream(new File("data/test/formatedData"+date+".dat"));
//        FileOutputStream fout = new FileOutputStream(new File("data/test/formatedDataBigTest.dat"));
        int count = 0;
        int max = setHashMap();

        System.out.println("Max: "+max);
        while(in.ready())
        {
            buf = in.readLine();

            //get dev ID
            String[] splittedData = buf.split("\t");
            long devId = Long.parseLong(splittedData[0]);

            //get app array
            String[] sarray = splittedData[1].split(",");
            int[] apps = new int[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                apps[i] = Integer.parseInt(sarray[i]);
            }

            //count for apps
            int[] appCount = new int[max+1];
            for (int i = 0; i < appCount.length; i++) {
                appCount[i] = 0;
            }
            int cateId;
            int total = 0;
            for (int i = 0; i < apps.length; i++) {
                cateId = appTagMap.getOrDefault(apps[i], -1);
                if (cateId != -1) {
                    if(appCount[cateId] == 0) {
                        total += 1;
                    }
                    appCount[cateId] = appCount[cateId] + 1;
                }
            }
            fout.write(Integer.toString(total).getBytes());
            for (int i = 0; i < appCount.length; i++){
                if(appCount[i] != 0) {
                    fout.write((" " + i + ":" + appCount[i]).getBytes());
                }
            }
            fout.write("\r\n".getBytes());
            count += 1;
            if(count % 1000 == 0){
                System.out.println("Count: " + count);
            }
        }
        System.out.println(count);
    }
}
