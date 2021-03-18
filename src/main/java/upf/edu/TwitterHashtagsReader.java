package upf.edu;

import java.util.*;
import upf.edu.model.HashTagCount;
import upf.edu.storage.DynamoHashTagRepository;

public class TwitterHashtagsReader {

    public static void main(String[] args) throws Exception {
        String lang = args[0];

        DynamoHashTagRepository w = new DynamoHashTagRepository();
        List<HashTagCount> top10= w.readTop10(lang);

        //Printing the top10 hashtags of the given language.
        System.out.println("Top 10 Hashtags in: "+lang);
        System.out.println("\nHashtag | Counter\n");
        
        for(int i=0; i<top10.size(); i++)
            System.out.println(top10.get(i).getHashTag()+" | "+top10.get(i).getCount());
    }
}