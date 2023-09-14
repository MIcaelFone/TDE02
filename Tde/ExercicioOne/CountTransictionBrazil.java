package Tde.ExercicioOne;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Objects;

public class CountTransictionBrazil {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("tde.output/ExercicioOne");
        Job j = new Job(c, "CountTransictionBrazil");
        j.setJarByClass(CountTransictionBrazil.class); // Classe que tem o main
        j.setMapperClass(MapForTransitionBrazilCount.class); // Classe do MAP
        j.setReducerClass(ReduceTransitionBrazilCount.class); // Classe do Reduce
        j.setCombinerClass(CombinerForTransitionBrazilCount.class); // Combiner igual ao Reduce!
        j.setMapOutputKeyClass(Text.class); // chave
        j.setMapOutputValueClass(IntWritable.class); // valor
        j.setOutputKeyClass(Text.class); // chave
        j.setOutputValueClass(IntWritable.class); // valor
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(false);
    }
    public static class MapForTransitionBrazilCount extends Mapper<LongWritable, Text,
            Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String country = value.toString();
            String[] pais_requirido = country.split(";");
            if (Objects.equals(pais_requirido[0], "Brazil")) {
                con.write(new Text(pais_requirido[0]), new IntWritable(1));
            }
        }
    }
        public static class CombinerForTransitionBrazilCount extends Reducer<Text, IntWritable, Text,
                IntWritable>{
            public void reduce(Text key, Iterable<IntWritable> values, Context con)
                    throws IOException, InterruptedException {
                int soma = 0;
                for (IntWritable v : values){
                    soma += v.get();
                }
                con.write(key, new IntWritable(soma));
            }
        }
        public static class ReduceTransitionBrazilCount extends Reducer<Text, IntWritable, Text,
                IntWritable> {
            public void reduce(Text key, Iterable<IntWritable> values, Context con)
                    throws IOException, InterruptedException {
// loop para somar os valores da lista
                int soma = 0;
                for (IntWritable v : values){
                    soma += v.get();
                }
// salvando em arquivo
                con.write(key, new IntWritable(soma));
            }
        }

    }







// loop pra criar tuplas (chave=p, valor=1)




