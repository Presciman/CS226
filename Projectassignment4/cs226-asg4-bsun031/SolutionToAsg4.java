import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.collection.Seq;

import java.io.*;
import java.util.List;

public class SolutionToAsg4 {

    public static void main(String[] args) throws IOException {
        //TODO Initialize Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("bsun031_asg4")
                .config("spark.master", "local")
                .getOrCreate();

        //String path = "file:///Users/ericsun/IdeaProjects/cs266-asg4-bsun031/input/nasa.tsv";
        if(args.length != 3)
        {
            System.err.println("Please input #1 Path, #2 start timestamp and #3 end timestamp given to taskB!");
            System.exit(128);
        }
        String path = args[0];
        long start_time = Long.parseLong(args[1]);
        //long start_time = 804571201;
        //long end_time = 804571214;
        long end_time = Long.parseLong(args[2]);
        //TODO Read Data
        Dataset<Row> df = readData(spark,path);
        //TODO taskA: Find the average
        taskA(df);
        //TODO taskB: Find the count
        taskB(df,start_time,end_time);
        //TODO output taskA.txt, taskB.txt and taskC.txt
    }
    public static Dataset<Row> readData(SparkSession spark, String path){
        long start_time = System.currentTimeMillis();
        //TODO read data
        StructType nasaSchema = new StructType(new StructField[]{
                new StructField("host", DataTypes.StringType, false, Metadata.empty()),
                new StructField("logname", DataTypes.StringType, false, Metadata.empty()),
                new StructField("time", DataTypes.LongType, false, Metadata.empty()),
                new StructField("method", DataTypes.StringType, false, Metadata.empty()),
                new StructField("URL", DataTypes.StringType, false, Metadata.empty()),
                new StructField("response", DataTypes.StringType, false, Metadata.empty()),
                new StructField("bytes", DataTypes.DoubleType, false, Metadata.empty()),
                //new StructField("referrer", DataTypes.StringType, false, Metadata.empty()),
                //new StructField("useragent", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header","false")
                .option("delimiter","\t")
                .schema(nasaSchema)
                .load(path);
        //.toDF("host","logname","time","method","URL","response","bytes");
        long end_time = System.currentTimeMillis();
        long time_spent = end_time - start_time;
        System.out.println("Total reading time: "+time_spent+" ms");
        return df;
    }
    public static void taskA(Dataset<Row> df) throws IOException {

        long start_time = System.currentTimeMillis();
        Dataset<Row> res = df.groupBy("response").mean("bytes");
        //res.show();

        File writeFile = new File("taskA.txt");
        if(writeFile.exists())
        {
            writeFile.delete();
            writeFile.createNewFile();
        }
        else {
            writeFile.createNewFile();
        }

        BufferedWriter write = new BufferedWriter(new FileWriter(writeFile));
        write.write("Spark SQL result:\r\n");
        //transform to RDD
        /*JavaRDD<Row> rdd = res.toJavaRDD();
        rdd.foreach(new VoidFunction<Row>() {
                        public void call(Row row) throws Exception {
                            //TODO Get the final result
                            write.write("Code "+ row.get(0)+", average number of bytes = "+row.get(1)+"\r\n");
                            //System.out.println("Code "+ row.get(0)+", average number of bytes = "+row.get(1));
                        }
                    }

        );*/
        List<Row> res_list = res.collectAsList();
        for(Row row: res_list)
        {
            write.write("Code "+ row.get(0)+", average number of bytes = "+row.get(1)+"\r\n");
            System.out.println("Code "+ row.get(0)+", average number of bytes = "+row.get(1));
        }
        long end_time = System.currentTimeMillis();
        long time_spent = end_time - start_time;
        write.write("\r\n");
        write.flush();
        write.close();
        System.out.println("Total taskA time: "+time_spent+" ms");
    }
    public static void taskB(Dataset<Row> df, long start_time, long end_time) throws IOException {
        long start_systime = System.currentTimeMillis();
        //TODO make sure the end_time is later than start_time
        if(end_time < start_time)
        {
            long temp_time;
            temp_time = end_time;
            end_time = start_time;
            start_time = temp_time;
        }
        //df.sort("time").show();
        long res = df.filter("time >= "+start_time).filter("time <= "+end_time).count();
        System.out.println("There are "+res+" log entries that happen between "+start_time+" and "+ end_time);
        long end_systime = System.currentTimeMillis();
        long time_spent = end_systime - start_systime;
        File writeFile = new File("taskB.txt");
        if(writeFile.exists())
        {
            writeFile.delete();
            writeFile.createNewFile();
        }
        else {
            writeFile.createNewFile();
        }

        BufferedWriter write = new BufferedWriter(new FileWriter(writeFile));
        write.write("Spark SQL result:\r\n");
        System.out.println("Total taskB time: "+time_spent+" ms");
        write.write("There are "+res+" log entries that happen between "+start_time+" and "+ end_time);
        write.write("\r\n");
        write.flush();
        write.close();
    }
        //SQL++
        /*#/bin/bash
curl -v --data-urlencode "statement=
       create type NasaType as closed {
            host: string,
                    logname: string,
                    time: bigint,
            method: string,
                    URL: string,
                    response: string,
                    bytes: double,
            referrer: string,
                    useragent: string
        };
        create external dataset Nasa(NasaType)
                using localfs
                ((\"path\"=\"127.0.0.1://Users/ericsun/nasa.tsv\"),
                        (\"format\"=\"delimited-text\"),
                        (\"delimiter\"=\"\t\"));
FROM Nasa GROUP BY response SELECT AVG(bytes) AS average, response;" \
          --data pretty=true                     \
          --data client_context_id=xyz           \
          http://localhost:19002/query/service
         */
        /*$ curl -v --data-urlencode "statement=select 1;" \
        --data pretty=true                     \
        --data client_context_id=xyz           \
        http://localhost:19002/query/service*/

}
