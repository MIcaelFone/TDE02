package Tde.ExercicioThree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;
public class AverageOfCommodityValuesPerYear {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("tde.output/ExercicioThree");
        Job j = new Job(c, "AvagereOfCommodityPerYear");
        j.setJarByClass(AverageOfCommodityValuesPerYear.class); // Classe que tem o main
        j.setMapperClass(MapForAverageOfCommodityValuesPerYear.class); // Classe do MAP
        j.setReducerClass(ReduceAvangereOfCommodityValuesPerYear.class); // Classe do Reduce
        j.setCombinerClass(CombinerAverageofCommodityValuesPerYear.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ReaptandSomaWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(false);
    }
    public static class MapForAverageOfCommodityValuesPerYear extends Mapper<LongWritable, Text, Text, ReaptandSomaWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] parts = linha.split(";");

            String Year = parts[1];
            String price = parts[5];


            if (!price.equals("trade_usd")) {
                try {
                    double preco = Double.parseDouble(price);
                    con.write(new Text(Year), new ReaptandSomaWritable(1, preco));
                } catch (NumberFormatException e) {
                    // Handle the case where price cannot be parsed as a float
                    // You may log this error or handle it as needed
                }
            }
        }



    }


    public static class CombinerAverageofCommodityValuesPerYear extends Reducer<Text, ReaptandSomaWritable, Text, ReaptandSomaWritable> {
           public void reduce(Text key, Iterable<ReaptandSomaWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma_repeat = 0;
            double soma_valores = 0;
            for (ReaptandSomaWritable v : values) {
                soma_repeat += v.getReapt();
                soma_valores += v.getPreco();

            }
            con.write(key, new ReaptandSomaWritable(soma_repeat,soma_valores));

        }
    }
     public static class ReduceAvangereOfCommodityValuesPerYear extends Reducer<Text, ReaptandSomaWritable, Text,DoubleWritable> {
        public void reduce(Text key, Iterable<ReaptandSomaWritable> values, Context con)
                throws IOException, InterruptedException {
            double soma_valores = 0;
            int soma_repeat = 0;
            for (ReaptandSomaWritable v : values) {
                soma_valores += v.getPreco();
                soma_repeat += v.getReapt();
            }
            double media = soma_valores / soma_repeat;

            con.write(key, new DoubleWritable(media));
        }
    }

}



