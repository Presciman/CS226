
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple7;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class SparkRDDasg {
    public static class AverageTuple implements Serializable {
        private int count;
        private double average;

        public AverageTuple(int count, double average){
            super();
            this.count = count;
            this.average = average;
        }

        public int getCount() {
            return count;
        }
        public void setCount(int count){
            this.count = count;
        }
        public double getAverage(){
            return average;
        }
        public void setAverage(double average) {
            this.average = average;
        }
    }


    public static void main(String[] args) throws IOException {
        if(args.length == 0)
        {
            System.err.println("Please add the argument of input file location");
            System.exit(149);
        }
        String inputDirectory = args[0];
        File file = new File(inputDirectory);
        if(!file.exists())
        {
            System.err.println("FILE NOT FOUND. Please input a valid file location");
            System.exit(404);
        }
//        String outputDirectory = args[1];
        long startTime1 = System.currentTimeMillis();
        //TODO Initialize Spark
        JavaSparkContext spark = new JavaSparkContext("local","CS266-SparkRDD");
        //TODO Read the datafile
        JavaRDD<String> fileRDD = spark.textFile(inputDirectory); //"inputFile/nasa.tsv"
        long endTime1 = System.currentTimeMillis();
        //TODO get average number for each response code
        long startTime2 = System.currentTimeMillis();
        JavaPairRDD<String,AverageTuple> bytebyCode1 = fileRDD.mapToPair(new PairFunction<String, String, AverageTuple>() {
                                                                            public Tuple2<String, AverageTuple> call(String value) throws Exception {
                                                                                String data = value.toString();
                                                                                String[] field = data.split("\t");

                                                                                if(field != null){
                                                                                    return new Tuple2<String, AverageTuple>(field[5], new AverageTuple(1,Double.parseDouble(field[6])));
                                                                                }
                                                                                return new Tuple2<String, AverageTuple>("Invalid_Record", new AverageTuple(0, 0.0));
                                                                            }
                                                                        });
        JavaPairRDD<String, AverageTuple> result1 = bytebyCode1.reduceByKey(new Function2<AverageTuple, AverageTuple, AverageTuple>() {
            public AverageTuple call(AverageTuple result, AverageTuple value) throws Exception {
                result.setAverage(result.getAverage() + value.getAverage());
                result.setCount(result.getCount() + value.getCount());
                return result;
            }
        });
        long endTime2 = System.currentTimeMillis();
        //TODO Output the result to File
        File task1 = new File("task1.txt");
        task1.createNewFile();
        BufferedWriter out1 = new BufferedWriter(new FileWriter(task1));
        for(Tuple2<String, AverageTuple> s : result1.collect())
        {
            String resultStr1 = "Code " + s._1 + ", average number of bytes = " + s._2.getAverage()/s._2.getCount();
            out1.write(resultStr1+"\r\n");
        }
        out1.close();
        long startTime3 = System.currentTimeMillis();
        //TODO Find pairs of request
        JavaPairRDD<Tuple2, Tuple7> result2_1 = fileRDD.mapToPair(new PairFunction<String, Tuple2, Tuple7>() {
            public Tuple2<Tuple2, Tuple7> call(String s) throws Exception {
                String data = s.toString();
                String[] field = data.split("\t");
                if(field != null){
                    return new Tuple2<Tuple2, Tuple7>(new Tuple2<String, String>(field[0],field[4]), new Tuple7<String, String, Long, String, String, String, Double>(field[0], field[1], Long.parseLong(field[2]),field[3],field[4],field[5], Double.parseDouble(field[6])));
                }
                return null;
            }
        });
        JavaPairRDD<Tuple2, Tuple7> result2_2 = fileRDD.mapToPair(new PairFunction<String, Tuple2, Tuple7>() {
            public Tuple2<Tuple2, Tuple7> call(String s) throws Exception {
                String data = s.toString();
                String[] field = data.split("\t");
                if(field != null){
                    return new Tuple2<Tuple2, Tuple7>(new Tuple2<String, String>(field[0],field[4]), new Tuple7<String, String, Long, String, String, String, Double>(field[0], field[1], Long.parseLong(field[2]),field[3],field[4],field[5], Double.parseDouble(field[6])));
                }
                return null;
            }
        });
        JavaPairRDD<Tuple2, Tuple2<Tuple7, Tuple7>> joinedByHostUrl = result2_1.join(result2_2);

        //Filter
        JavaPairRDD<Tuple2, Tuple2<Tuple7, Tuple7>> filteredResult= joinedByHostUrl.filter(new Function<Tuple2<Tuple2, Tuple2<Tuple7, Tuple7>>, Boolean>() {
            public Boolean call(Tuple2<Tuple2, Tuple2<Tuple7, Tuple7>> t) throws Exception {
                return (!t._2._1.equals(t._2._2)) && (Math.abs(Double.parseDouble(t._2._1._3().toString()) - Double.parseDouble(t._2._2._3().toString())) <= 3600) && (Double.parseDouble(t._2._1._3().toString()) - Double.parseDouble(t._2._2._3().toString()) > 0);
            }
        });
        long endTime3 = System.currentTimeMillis();
        String runningTimes = "Time for initializing Spark and read the data file: "+(endTime1-startTime1)+" (ms)\r\n" +
                              "Time for processing the average number: "+(endTime2-startTime2)+" (ms)\r\n" +
                              "Time for processing the desired pairs results: "+(endTime3-startTime3)+" (ms)\r\n";
        System.out.println(runningTimes);
        int count = 0;
        File task2 = new File("task2.txt");
        task2.createNewFile();
        BufferedWriter out2 = new BufferedWriter(new FileWriter(task2));
        for(Tuple2<Tuple2, Tuple2<Tuple7, Tuple7>> t : filteredResult.collect())
        {

            out2.write(t._2._1.toString()+"\r\n");
            out2.write(t._2._2.toString()+"\r\n");
            out2.write("*********************************************");
                count++;
        }
        out2.write("Total "+count+" records");
        out2.close();

    }
}
