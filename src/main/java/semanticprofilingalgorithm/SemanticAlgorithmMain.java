package semanticprofilingalgorithm;

import org.apache.hadoop.conf.Configured;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;
import java.io.IOException;
import java.sql.*;

public class SemanticAlgorithmMain extends Configured {

    /**
     * Initialization
     */

    public static void main(String[] args) throws IOException, SQLException {
        System.setProperty("hive.metastore.uris", "thrift://sandbox.kylo.io:9083");
        //System.setProperty("hhive.scratch.dir.permission","777");


        final SparkConf conf = new SparkConf().setAppName("SparkHive")

                .setMaster("local").setSparkHome("/usr/hdp/current/spark-client");
        //hive-site.xml mapped to our conf object
        conf.set("spark.yarn.dist.files", "file:/usr/hdp/2.5.6.0-40/spark/conf/hive-site.xml");
        //connect to our existing hive metastore version to avoid creation of new db instance
        conf.set("spark.sql.hive.metastore.version", "1.2.1");
        SparkContext sc = new SparkContext(conf);

        HiveContext hiveContext = new HiveContext(sc);

        //connect spark to hive through thrift api
        hiveContext.setConf("hive.metastore.uris", "thrift://sandbox.kylo.io:9083");

        String db = args[0];
        String table = args[1];
        String timestamp = args[2];

        //generate data dictionary at table level
        new DataDictionary().generateSchemaDataDictionary(db, hiveContext, table, timestamp);

        //generate data dictionary at record level
        new DataDictionary().generateInstanceDataDictionary(db, hiveContext, table,timestamp);

        //generate dominant ontology for each column based on data dictionary
        new DominantOntology().generateDominantOntology(db, hiveContext, table, timestamp);

        //generate Semantic Indicators - no match, partial match and full match
        new SemanticIndicators().generateSemanticIndicators(db, hiveContext, table, timestamp);
    }


}