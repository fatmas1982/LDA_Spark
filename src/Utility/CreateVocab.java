package Utility;

import java.io.*;

import static java.lang.Integer.parseInt;

/**
 * Created by liyan on 9/21/15.
 */
public class CreateVocab {

    public static void main(String[] args) throws IOException{
        BufferedReader in = new BufferedReader(new FileReader(new File("data/data20150921/apk_cates.log")));
        FileOutputStream fout = new FileOutputStream(new File("verification/newvocab.txt"));
        String buf;
        int count = 1;
        while (in.ready()){
            buf = in.readLine();
            String[] splitData = buf.split(",");
            while(parseInt(splitData[0]) != count){
                count += 1;
                fout.write("NULL\r\n".getBytes());
            }
            fout.write((splitData[1] + "\r\n").getBytes());
            count += 1;
        }
        fout.close();
    }
}
