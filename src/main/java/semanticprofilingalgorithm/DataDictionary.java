package semanticprofilingalgorithm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

public class DataDictionary {
    static final String REST_URL = "http://data.bioontology.org";
    //static final String API_KEY = "83ec6817-48e5-434b-b087-6ea879f424a3";
    static String API_KEY;
    static final ObjectMapper mapper = new ObjectMapper();

    void generateSchemaDataDictionary(String db, SQLContext sqlContext, String table, String timestamp)
    {

        String df_query= "select * from " + db +"."+ table+"_valid";
        DataFrame df = sqlContext.sql(df_query).toDF();

        StructType schema= df.schema();
        String query="CREATE TABLE IF NOT EXISTS " + db+ "."
                + "DD_Schema "
                + "(feed_name string, "
                + "field_name string, "
                + "description string, "
                + "preferred_type string, "
                + "preferred_label string, "
                + "ontology_uri string, "
                + "ts string) ";

        DataFrame dd = sqlContext.sql(query);

        StructField[] df_struct= df.schema().fields();

        JsonNode resources;
        for (int i = 0; i < df_struct.length; i++) {

            //Get the available resources
            String resourcesString = get(REST_URL + "/recommender?input="
                    + df_struct[i].name().replace("_","+"));

            resources = jsonToNode(resourcesString);
            JsonNode node= resources.get(0);
            String desc_url= node.get("coverageResult").get("annotations").get(0).get("annotatedClass").get("links").findValue("self").asText();



            // Get the ontologies from the link we found
            JsonNode desc_node = jsonToNode(get(desc_url));


            String desc=  ((desc_node.findValue("definition").get(0)) == null)
                    ? "" : (desc_node.findValue("definition").get(0).asText());

            String pref_name = node.get("coverageResult")
                    .get("annotations")
                    .get(0)
                    .findValue("text").asText();

            String pref_type = node.get("coverageResult")
                    .get("annotations")
                    .get(0)
                    .findValue("matchType").asText();

            String ont_uri= node.get("ontologies")
                    .get(0)
                    .findValue("@id").asText();
            String field_name=df_struct[i].name();

            String DD_Schema_insert="select "+
                    "'" + table+"'" + " as feed_name, "+
                    "'" +field_name +"'" + " as field_name, "+
                    "\""+desc+ "\""+ " as description, "+
                    "'"+pref_type+ "'"+ " as preferred_type, "+
                    "'"+pref_name+ "'"+ " as preferred_label, " +
                    "'"+ont_uri+ "'"+ " as ontology_uri, " +
                    "'"+timestamp+ "'"+ " as ts ";


            DataFrame data= sqlContext.sql(DD_Schema_insert);

            data.write().mode("append").saveAsTable(db+ "."+ "dd_schema");

        }
    }

    void generateInstanceDataDictionary(String db, SQLContext sqlContext, String table, String timestamp)
    {
        String df_query= "select * from " + db +"."+ table+"_valid";
        DataFrame df = sqlContext.sql(df_query).toDF();

        String query="CREATE TABLE IF NOT EXISTS " + db+ "."
                + "dd_instance "
                + "(feed_name string, "
                + "field_name string, "
                + "field_value string, "
                + "frequency int, "
                + "description string, "
                + "preferred_type string, "
                + "preferred_label string, "
                + "ontology_uri string, "
                + "ts string) ";

        JsonNode resources;

        sqlContext.sql(query);

        //    StructField[] df_struct= df.schema().fields();
        for (int i = 0; i < df.columns().length; i++) {
            Row[] r=df.groupBy(df.columns()[i]).count().collect();

            String resourceString="";

            for (int i1 = 0; i1 < r.length; i1++) {

                if (r[i1].get(0).toString().equals("") || r[i1].get(0).toString() == null) {
                    continue;
                }
                resourceString= get(REST_URL + "/recommender?input=" +r[i1].get(0).toString());

                if (resourceString.equals("[]") || resourceString == null) {
                    continue;
                }
                resources = jsonToNode(resourceString);
                JsonNode node= resources.get(0);

                String desc_url= ((node
                        .get("coverageResult")
                        .get("annotations")
                        .get(0)
                        .get("annotatedClass")
                        .get("links")
                        .findValue("self"))==null)
                        ? "" :
                        (node.get("coverageResult")
                                .get("annotations")
                                .get(0)
                                .get("annotatedClass")
                                .get("links")
                                .findValue("self")).asText();

                // Get the ontologies from the link we found
                JsonNode desc_node = jsonToNode(get(desc_url));
                /*String desc=  ((desc_node.findValue("definition").get(0)) == null)
                        ? "" : (desc_node.findValue("definition").get(0).asText());
                */
                String desc;
                if(desc_node.findValue("definition").get(0) != null)
                    desc =desc_node.findValue("definition").get(0).asText();
                else if(desc_node.findValue("prefLabel") != null)
                    desc= desc_node.findValue("prefLabel").asText();
                else
                    desc="";


                String pref_name = node.get("coverageResult")
                        .get("annotations")
                        .get(0)
                        .findValue("text").asText();


                String pref_type = node.get("coverageResult")
                        .get("annotations")
                        .get(0)
                        .findValue("matchType").asText();

                String ont_uri= node.get("ontologies")
                        .get(0)
                        .findValue("@id").asText();

                String DD_Instance_insert="select "+
                        "'" + table+"'" + " as feed_name, "+
                        "'" + df.schema().fields()[i].name()+"'" + " as field_name, "+
                        "'" + r[i1].get(0).toString()+"'" + " as field_value, "+
                        Integer.parseInt(r[i1].get(1).toString())+ " as frequency, "+
                        "'"+desc+ "'"+ " as description, "+
                        "'"+pref_type+ "'"+ " as preferred_type, "+
                        "'"+pref_name+ "'"+ " as preferred_label, " +
                        "'"+ont_uri+ "'"+ " as ontology_uri, "+
                        "'"+timestamp+ "'"+ " as ts";


                DataFrame data= sqlContext.sql(DD_Instance_insert);

                data.write().mode("append").saveAsTable(db+ "."+ "dd_instance");
            }


        }

    }


    private static JsonNode jsonToNode(String json) {
        JsonNode root = null;
        try {
            root = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return root;
    }

    private static String get(String urlToGet) {
        URL url;
        HttpURLConnection conn;
        BufferedReader rd;
        String line;
        String result = "";
        try {
            url = new URL(urlToGet);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "apikey token=" + API_KEY);
            conn.setRequestProperty("Accept", "application/json");
            rd = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


}