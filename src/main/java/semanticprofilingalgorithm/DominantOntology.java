package semanticprofilingalgorithm;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.sum;

public class DominantOntology {
    void generateDominantOntology(String db, SQLContext sqlContext, String table, String timestamp)
    {
        //correct this logic frequency*count
        String df_query= "select  field_name, ontology_uri, frequency*count(ontology_uri) from " + db+"."
                + "dd_instance"
                +" where " + "feed_name="+ "'"+ table +"'"
                +" and ts="+"'"+timestamp+"'"
                + " GROUP BY field_name,frequency,ontology_uri";
        DataFrame df = sqlContext.sql(df_query)
                .toDF("field_name", "ontology_uri", "frequency_count");

        /*Row[] dom_results = df.groupBy("field_name","ontology_uri")
                .agg(functions.sum(("frequency_count"))).collect();*/

        Row[] dom_results = df.groupBy("field_name","ontology_uri")
                .agg(sum(("frequency_count"))).collect();


        String query="CREATE TABLE IF NOT EXISTS " + db+ "."
                + "dominant_ontology "
                + "(feed_name string, "
                + "field_name string, "
                + "ontology_uri string, "
                + "ts string)";

        sqlContext.sql(query);

        df.groupBy("field_name", "ontology_uri").agg(functions.count("frequency_count")).collect();
        for (int i = 0; i < dom_results.length; i++) {
            String DO_Instance_insert="select "+
                    "'" + table+"'" + " as feed_name, "+
                    "'" + dom_results[i].get(0)+"'" + " as field_name, "+
                    "'"+dom_results[i].get(1)+ "'"+ " as ontology_uri, "+
                    "'"+timestamp+ "'"+ " as ts ";

            DataFrame data= sqlContext.sql(DO_Instance_insert);

            data.write().mode("append").saveAsTable(db+ "."+ "dominant_ontology");
        }



    }
}
