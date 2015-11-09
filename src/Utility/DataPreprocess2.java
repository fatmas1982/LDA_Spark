package Utility;

import java.io.*;

/**
 * Created by liyan on 9/21/15.
 */
public class DataPreprocess2 {

    public static void main(String[] args) throws IOException{
        BufferedReader in = new BufferedReader(new FileReader(new File("data/data20150921/forbfd_usrpkgs_100000.log")));
        String buf;
        FileOutputStream fout = new FileOutputStream(new File("data/data20150921/apk_list.log"));
        while(in.ready()){
            buf = in.readLine();
            String[] splitData = buf.split("\t");
            if(!splitData[1].isEmpty()) {
                fout.write((splitData[0] + "\t" + splitData[1] + "\r\n").getBytes());
            }
        }
        fout.close();
    }
}
