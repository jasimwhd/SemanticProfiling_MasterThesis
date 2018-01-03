package semanticprofilingalgorithm;

import antlr.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;

import java.util.*;


public class SemanticIndicators {
    void generateSemanticIndicators(String db, SQLContext
            sqlContext, String table, String timestamp) {
        //---------------------**generate no match indicator **-----------------------------------
        //those attributes that do not qualify in DD_instance are automatically considered for no match indicator
        DataFrame df = sqlContext.sql("select * from " + db + "." + table
                + "_valid where processing_dttm=" +
                "'" + timestamp + "'").toDF();
        List<String> base = Arrays.asList(df.columns());
        //check if attribute contains reserve word like hyphen, if exists, add backticks to make it assessible
        String query_semantic = df.toString().replace(":", "")
                .replace("[", "(").replace("]", ")")
                .replaceAll("(\\w+)([\\-])(\\w+)", "`" + "$1$2$3" + "`");
        String query = "CREATE TABLE IF NOT EXISTS " + db + "."
                + table + "_semantic_no_match " + query_semantic;
        sqlContext.sql(query);
        DataFrame lookup = sqlContext.sql("select distinct(field_name) " +
                " from " + db + "." + "dd_instance "
                + " where feed_name=" + "'" + table + "'"
                + " and ts=" + "'" + timestamp + "'").toDF();
        List<String> lu = lookup.as(Encoders.STRING()).collectAsList();
        int i = 0;
        for (String x : base) {
            for (String subx : lu)
                if (x.equals(subx))
                    base.set(i, "NULL");
            i++;
        }
        String att = org.apache.commons.lang.StringUtils.join(base, ",");
        att = att.replaceAll("(\\w+)([\\-])(\\w+)", "`" + "$1$2$3" + "`");
        //semantic_no_match
        sqlContext.sql("insert into table " + db + "." + table
                + "_semantic_no_match " +
                " select " + att
                + " from "
                + db + "." + table
                + "_valid where processing_dttm=" +
                "'" + timestamp + "'"
        );

        //---------------------**generate valid match indicator **-----------------------------------
        DataFrame df_valid = sqlContext.sql("select distinct " +
                "dd_instance.field_name,dd_instance.field_value, dd_instance.frequency, dd_instance.ts" +
                " from "+db +"."+ "dd_instance, "+ db +"."+ "dominant_ontology" +
                " where dd_instance.field_value=dd_instance.preferred_label and " +
                " dd_instance.ontology_uri=dominant_ontology.ontology_uri" +
                " and dd_instance.feed_name="+ "'"+ table+"'" +
                " and dd_instance.preferred_type='PREF'" +
                " and dd_instance.ts="+ "'"+ timestamp+"'")
                .toDF("field_name","field_value","frequency" ,"processing_dttm");


        String query_valid = "CREATE TABLE IF NOT EXISTS " + db + "."
                + table + "_semantic_valid_match " + "(field_name string, field_value string, frequency int, processing_dttm string )";

        sqlContext.sql(query_valid);


        df_valid.select(df_valid.col("field_name"),df_valid.col("field_value"),df_valid.col("frequency"),df_valid.col("processing_dttm"))
                .write()
                .mode("append")
                .saveAsTable(db+ "."+ table + "_semantic_valid_match ");
        //---------------------**generate partial match indicator **-----------------------------------

        /*DataFrame df_partial = sqlContext.sql("select " +
                "dd_instance.field_name,dd_instance.field_value, dd_instance.frequency, dd_instance.ts" +
                " from "+db +"."+ "dd_instance, "+ db +"."+ "dominant_ontology" +
                " where dd_instance.ontology_uri=dominant_ontology.ontology_uri" +
                " and dd_instance.feed_name="+ "'"+ table+"'" +
                " and dd_instance.preferred_type='SYN'" +
                " and dd_instance.ts="+ "'"+ timestamp+"'")
                .toDF("field_name","field_value", "frequency", "processing_dttm");*/

        DataFrame df_partial = sqlContext.sql("SELECT t1.* FROM (select field_name,field_value,frequency,ts " +
                " from study1.dd_instance where feed_name="+ "'"+table+ "'"+
                " and ts="+"'"+timestamp+"'" + ") t1 " +
                " LEFT OUTER JOIN (select field_name,field_value,frequency,processing_dttm " +
                " from " + db+"."+ table+"_semantic_valid_match"+ ") t2 " +
                " ON (t1.field_name=t2.field_name AND t1.field_value=t2.field_value) " +
                " WHERE t2.field_name IS NULL OR t2.field_value IS NULL");

        //if dominant ontology is indeed a synonymous value itself, capture only synonymous values from data dictionary

        if(df_partial.collect().length==0)
            df_partial=sqlContext.sql("select distinct " +
                    "dd_instance.field_name,dd_instance.field_value, dd_instance.frequency, dd_instance.ts" +
                    " from "+db +"."+ "dd_instance, "+ db +"."+ "dominant_ontology" +
                    " where  " +
                    " dd_instance.ontology_uri=dominant_ontology.ontology_uri" +
                    " and dd_instance.feed_name="+ "'"+ table+"'" +
                    " and dd_instance.ts="+ "'"+ timestamp+"'")
                    .toDF("field_name","field_value","frequency" ,"ts");

        String query_partial = "CREATE TABLE IF NOT EXISTS " + db + "."
                + table + "_semantic_partial_valid_match " +
                "(field_name string, field_value string, frequency int, processing_dttm string )";

        sqlContext.sql(query_partial);


        df_partial.select(df_partial.col("field_name"),df_partial.col("field_value"),df_partial.col("frequency"),df_partial.col("ts"))
                .write()
                .mode("append")
                .saveAsTable(db+ "."+ table + "_semantic_partial_valid_match ");

    }
}
