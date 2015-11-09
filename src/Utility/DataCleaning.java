package Utility;

import java.io.*;
import java.util.HashMap;

public class DataCleaning {

    static HashMap<String, Boolean> stopWords;

    public static void setHashMap() throws IOException {
        // Set up HashMap
        stopWords = new HashMap<String, Boolean>();
        BufferedReader in = new BufferedReader(new FileReader(new File("data/data20150921/stop_words.log")));
        String buf;
        while(in.ready())
        {
            buf = in.readLine();
            String[] splittedData = buf.split(" | ");
            stopWords.put(splittedData[0], true);
        }
        in.close();
    }

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		String date = "20141203";
//		File file = new File("data/realData/"+date+"/part-r-00000");
        File file = new File("data/data20150921/apk_list.log");
		BufferedReader in = new BufferedReader(new FileReader(file));
		String buf = null;
		int passageLength = 70;
		FileOutputStream fout = new FileOutputStream(new File("data/data20150921/cleaned_data_length_"+passageLength+".log"));
		int count = 0;
        setHashMap();
        boolean flag = false;
		while(in.ready())
		{
			buf = in.readLine();
			String[] splittedData = buf.split("\t");
			long devId = Long.parseLong(splittedData[0]);
			
			//get app array
			String[] sarray = splittedData[1].split(",");

			if(sarray.length > passageLength)
			{
                flag = false;
				fout.write(Long.toString(devId).getBytes());
				fout.write("\t".getBytes());
                if(!stopWords.getOrDefault(sarray[0], false)) {
                    fout.write(sarray[0].getBytes());
                    flag = true;
                }
				for (int i = 1; i < sarray.length; i++) {
                    if(!stopWords.getOrDefault(sarray[i], false)) {
                        if(flag == true) {
                            fout.write(",".getBytes());
                        }
                        fout.write(sarray[i].getBytes());
                        flag = true;
                    }
                }
				fout.write("\r\n".getBytes());
				count += 1;
				if(count % 1000 == 0){
                    System.out.println("Count: " + count);
                }
			}
		}
		System.out.println(count);
		in.close();
		fout.close();
	}

}
