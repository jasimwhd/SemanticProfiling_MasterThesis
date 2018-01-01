package semanticprofilingalgorithm;

import org.apache.hadoop.conf.Configured;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.io.IOException;
import java.sql.*;

public class SemanticAlgorithmMain extends Configured {

    /**
     * Initialization
     */
    final static Logger logger = Logger.getLogger(SemanticAlgorithmMain.class);

    public static void main(String[] args) {
        //logger.setLevel(Level.INFO);

        if (args[0] == null || args[0].equals("") ||
                args[1] == null || args[1].equals("") ||
                args[2] == null || args[2].equals("") ||
                args[3] == null || args[3].equals("")) {
            logger.info("One or more arguments are not properly defined");
            logger.info("First argument: category, Second argument: feed, Third argument: timestamp, Fourth argument: API_Key of BioPortal");
        } else {
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

            logger.info("Connection established with Metastore via Spark ");
            String db = args[0];
            String table = args[1];
            String timestamp = args[2];
            DataDictionary.API_KEY = args[3];

            //generate data dictionary at table level
            logger.info("Generating Data Dictionary at Schema Level...");
            //new DataDictionary().generateSchemaDataDictionary(db, hiveContext, table, timestamp);

            //generate data dictionary at record level
            logger.info("Generating Data Dictionary at Instance Level...");
            new DataDictionary().generateInstanceDataDictionary(db, hiveContext, table, timestamp);

            //generate dominant ontology for each column based on data dictionary
            logger.info("Generating Dominant Ontology for each column");
            new DominantOntology().generateDominantOntology(db, hiveContext, table, timestamp);

            //generate Semantic Indicators - no match, partial match and full match
            logger.info("Generating Semantic Indicators: partial, invalid, valid");
            new SemanticIndicators().generateSemanticIndicators(db, hiveContext, table, timestamp);
        }
        logger.info("Finished executing Semantic Profiling Algorithm!");
    }


}