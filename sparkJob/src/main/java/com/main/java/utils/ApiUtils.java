package com.main.java.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by rahul.tandon
 * Given AM url and headers output is the path of the rdds consisting of the employee for each categoryId
 */
public class ApiUtils {

    /**
     * @param url the url which is used to fetch the path of the HDFS input data
     * @param headerKey the headerKey which is used while fetching the output
     * @param headerValue the headerValue which is used while fetching the output
     * @return OutpathFilePath the HDFS path which contains the data for a categoryId
     * */
    public static String get(String url, String headerKey, String headerValue) throws IOException {

        URL urlForGetRequest = new URL(url);
        String readLine = null;
        HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
        conection.setRequestMethod("GET");
        conection.setRequestProperty(headerKey, headerValue);
        int responseCode = conection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(conection.getInputStream()));
            StringBuffer response = new StringBuffer();
            while ((readLine = in.readLine()) != null) {
                response.append(readLine);
            } in.close();
            String json_response = response.toString();
            String [] json_list = json_response.split(",");
            int i;
            for(i=0; i<json_list.length; i++)
                if(json_list[i].contains("location"))
                    break;
            String path = json_list[i].split(":")[1];
            return path.substring(1, path.length()-1);
        } else {
            throw new RuntimeException("no response" + responseCode);
        }
    }
}