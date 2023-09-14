package Tde.ExercicioSix;
import Tde.ExercicioOne.CountTransictionBrazil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class ExercicioSix {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("tde.output/ExercicioSixpart2");
        Job j = new Job(c, "HighestPriceofCountry");
        j.setJarByClass(ExercicioSix.class); // Classe que tem o main
        j.setMapperClass(MapHighestPrice.class); // Classe do MAP
        j.setReducerClass(ReducerHigestPrice.class); // Classe do Reduce
        j.setCombinerClass(CombinerHighestPrice.class); // Combiner igual ao Reduce!
        j.setMapOutputKeyClass(Text.class); // chave
        j.setMapOutputValueClass(DoubleWritable.class); // valor
        j.setOutputKeyClass(Text.class); // chave
        j.setOutputValueClass(DoubleWritable.class); // valor
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(false);
    }

    public static class MapHighestPrice extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] parts = linha.split(";");

            String Country = parts[0];
            String price = parts[5];


            if (!price.equals("trade_usd")) {
                try {
                    double preco = Double.parseDouble(price);
                    con.write(new Text(Country), new DoubleWritable(preco));
                } catch (NumberFormatException e) {
                    // Handle the case where price cannot be parsed as a float
                    // You may log this error or handle it as needed
                }
            }
        }

    }

    public static class CombinerHighestPrice extends Reducer<LongWritable, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {
            double max = 0;
            for (DoubleWritable v : values) {
                if (v.get() > max) {
                    max = v.get();
                }
            }
            con.write(key, new DoubleWritable(max));
        }


    }
    public static class ReducerHigestPrice extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException {
            double max = 0;
            for (DoubleWritable v : values) {
                if (v.get() > max) {
                    max = v.get();
                }
            }
            con.write(key, new DoubleWritable(max));
        }
}

}
