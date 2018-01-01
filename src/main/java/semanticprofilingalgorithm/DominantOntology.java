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
        /*String df_query= "select  field_name, ontology_uri, frequency*count(ontology_uri)" +
                " from " + db+"."
                + "dd_instance"
                +" where " + "feed_name="+ "'"+ table +"'"
                +" and ts="+"'"+timestamp+"'"
                + " GROUP BY field_name,frequency,ontology_uri";*/

        String df_query= "SELECT res.field_name, res.total_sum, res.ontology_uri " +
                "FROM \n" +
                "(select t1.field_name as field_name, \n" +
                "max(t1.total_sum) over (partition by t1.field_name,t1.ontology_uri order by t1.field_name asc) as total_sum, \n" +
                "t1.ontology_uri as ontology_uri, row_number() over (partition by t1.field_name order by t1.total_sum desc) as row_no \n" +
                "from \n" +
                "( \n" +
                "   select field_name, sum(frequency) as total_sum, ontology_uri \n" +
                "   from " + db +
                ".dd_instance \n" +
                "   where feed_name="+ "'"+table + "'"+
                " and ts="+"'" + timestamp +"'"+
                "   group by ontology_uri,field_name\n " +
                ") t1) res\n " +
                "WHERE res.row_no=1";
        DataFrame df = sqlContext.sql(df_query);


        /*Row[] dom_results = df.groupBy("field_name","ontology_uri")
                .agg(functions.sum(("frequency_count"))).collect();*/

        Row[] dom_results = df.collect();


        String query="CREATE TABLE IF NOT EXISTS " + db+ "."
                + "dominant_ontology "
                + "(feed_name string, "
                + "field_name string, "
                + "ontology_uri string, "
                + "ts string)";

        sqlContext.sql(query);

        for (int i = 0; i < dom_results.length; i++) {
            String DO_Instance_insert="select "+
                    "'" + table+"'" + " as feed_name, "+
                    "'" + dom_results[i].get(0)+"'" + " as field_name, "+
                    "'"+dom_results[i].get(2)+ "'"+ " as ontology_uri, "+
                    "'"+timestamp+ "'"+ " as ts ";

            DataFrame data= sqlContext.sql(DO_Instance_insert);

            data.write().mode("append").saveAsTable(db+ "."+ "dominant_ontology");
        }



    }
}
