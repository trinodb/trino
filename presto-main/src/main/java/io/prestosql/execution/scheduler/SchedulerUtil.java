package io.prestosql.execution.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchedulerUtil {
    public static String isReplaceStr(String str_content)
    {
        String r = "";
        Pattern p = Pattern.compile("\\s*|\t|\r|\n");
        Matcher m = p.matcher(str_content);
        r = m.replaceAll("");
        return r;
    }

    public static boolean isIPAddress(String str)
    {
        if(str.length()<7 || str.length() >15)
            return false;
        String[] arr = str.split("\\.");
        if( arr.length != 4 )
            return false;
        for(int i = 0 ; i <4 ; i++ ){
            for(int j = 0; j<arr[i].length();j++){
                char temp = arr[i].charAt(j);
                if(!( temp>'0' && temp< '9' ) )
                    return false;
            }
        }
        for(int i = 0 ; i<4;i++) {
            int temp = Integer.parseInt( arr[i] );
            if( temp<0 || temp >255)
                return false;
        }
        return true;
    }

    public static List<String> getBlackHosts(String pathname)
    {
        //String pathname = blackListPath;
        List<String> list = new ArrayList<>(8);

        if ((new File(pathname)).exists()) {
            try (FileReader reader = new FileReader(pathname); BufferedReader br = new BufferedReader(reader)) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = isReplaceStr(line);
                    if (isIPAddress(line)){
                        list.add(line);
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException("cannot read file: " + pathname, e);
            }
        }
        return list;
    }
}
