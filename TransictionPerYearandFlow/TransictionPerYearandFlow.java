package Tde.TransictionPerYearandFlow;
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

public class TransictionPerYearandFlow {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
        // arquivo de saida
        Path output = new Path("output/TransictionPerYearandFlow");
        // criacao do job e seu nome
        Job j = new Job(c, "TransictionPerYearandFlow");
// Registro de classes
        j.setJarByClass(TransictionPerYearandFlow.class); // Classe que tem o main
        j.setMapperClass(MapForTransitionTypeFlowandPerYear.class); // Classe do MAP
        j.setReducerClass(MapForTransitionTypeFlowandPerYear.ReduceTransitionTypeFlowandPerYear.class); // Classe do Reduce
        //Tipos de saida
        j.setMapOutputKeyClass(TypeFlowandPerYearWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        j.setCombinerClass(MapForTransitionTypeFlowandPerYear.Combiner.class);
        //Definição de arquivo de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);
        j.waitForCompletion(false);

// Rodar :)

    }

    public static class MapForTransitionTypeFlowandPerYear extends Mapper<LongWritable, Text, TypeFlowandPerYearWritable, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String Year = linha.split(";")[1];
            String Flow = linha.split(";")[4];
            con.write(new TypeFlowandPerYearWritable(Year,Flow), new IntWritable(1));

        }

        public static class Combiner extends Reducer<TypeFlowandPerYearWritable, IntWritable, TypeFlowandPerYearWritable,
                IntWritable> {
            public void reduce(TypeFlowandPerYearWritable key, Iterable<IntWritable> values, Context con)
                    throws IOException, InterruptedException {
// pré-somando valores em cada Mapper (computador que roda MAP)
                int soma=0;
                for (IntWritable v : values) {
                    soma += v.get();
                }
                con.write(new TypeFlowandPerYearWritable(key.getYear(),key.getFlow()), new IntWritable(soma));
            }
        }

        public static class ReduceTransitionTypeFlowandPerYear extends Reducer<TypeFlowandPerYearWritable, IntWritable, Text,
                IntWritable> {
            public void reduce (TypeFlowandPerYearWritable key, Iterable<IntWritable> values, Context con)
                    throws IOException, InterruptedException {
// pré-somando valores em cada Mapper (computador que roda MAP) e salvando em arquivo
                int soma = 0;
                for (IntWritable v : values) {
                    soma += v.get();
                }
                con.write(new Text(key.getYear()+" "+key.getFlow()), new IntWritable(soma));
            }
        }
    }
}



