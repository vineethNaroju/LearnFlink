package org.example;


import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;

import java.util.Date;

public class APIcall {


    public void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }


    public static void main(String[] args) throws Exception {

        APIcall api = new APIcall();
        api.poc();

    }

    public void poc() throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();

        String jph = "https://jsonplaceholder.typicode.com/todos";
        String google = "https://www.google.com";

        HttpGet req = new HttpGet(jph);


        CloseableHttpResponse res = httpClient.execute(req);

        print(res.getStatusLine().getStatusCode());


        HttpEntity entity = res.getEntity();


        // JSONObject json = new JSONObject(EntityUtils.toString(entity));

        JSONArray arr = new JSONArray(EntityUtils.toString(entity));


        print(arr);

        res.close();

    }

}
