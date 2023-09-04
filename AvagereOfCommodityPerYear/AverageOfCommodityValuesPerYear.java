package Tde.AvagereOfCommodityPerYear;
import Tde.TransictionPerYearandFlow.TransictionPerYearandFlow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import java.time.Year;
import java.util.Objects;

public class AverageOfCommodityValuesPerYear {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
// arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
// arquivo de saida
        Path output = new Path("output/AvagereOfCommodityPerYear");
// criacao do job e seu nome
        Job j = new Job(c, "AvagereOfCommodityPerYear");
// Registro de classes
        j.setJarByClass(AverageOfCommodityValuesPerYear.class);
        j.setMapperClass(MapForAvangereOfCommodityValuesPerYear.class);
        j.setReducerClass(ReduceAvangereOfCommodityValuesPerYear.class);
        j.setCombinerClass(CombinerAverageofCommodityValuesPerYear.class);
// Tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ReaptandSomaWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

// Definir tipos de saida
// Map

// Cadastrar arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
// Rodar :)
        j.waitForCompletion(false);
    }

    /**
     * Funcao de map é chamada por cada linha do arquivo de entrada
     * Dois tipos de entrega e dois tipos de saida
     * 1 info:tipo da chave de entrega (LongWritable)
     * 2info:tipo do valor de entrega (Text->linha do arquivo de texto)
     * 3info:tipo da chave de saida (Text-->palavra)
     * 4info tipo do valor de saida (IntWritable-->1)
     * a
     */
    public static class MapForAvangereOfCommodityValuesPerYear extends Mapper<LongWritable, Text, Text, ReaptandSomaWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String Year = linha.split(";")[1];
            String valueOfCommodity = linha.split(";")[7];
            con.write(new Text(Year), new ReaptandSomaWritable(1, Float.parseFloat(valueOfCommodity)));


        }
    }

    public static class CombinerAverageofCommodityValuesPerYear extends Reducer<Text, ReaptandSomaWritable, Text, ReaptandSomaWritable> {
        public void reduce(Text key, Iterable<ReaptandSomaWritable> values, Context con)
                throws IOException, InterruptedException {
            float soma_commidittes = 0.0f;
            int soma_repeat = 0;
            for (ReaptandSomaWritable v : values) {
                soma_commidittes += v.getSoma();
                soma_repeat += v.getReapt();
            }
            con.write(new Text(key), new ReaptandSomaWritable(soma_repeat, soma_commidittes));

        }
    }

    /**
     * Funcao de reduce é chamada por cada chave de entrada
     * Dois tipos de entrega e dois tipos de saida
     * 1 info:tipo da chave de entrega (PRECISA SER IGUAL AO TIPO DE CHAVE DE SAIDA DO MAP)
     * 2info:tipo do valor de entrega (Tb precisa casar com a saida do map)
     * 3info:tipo da chave de saida (Text-->palavra)
     * 4info tipo do valor de saida (IntWritable-->1)
     * a
     */
    public static class ReduceAvangereOfCommodityValuesPerYear extends Reducer<Text, ReaptandSomaWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<ReaptandSomaWritable> values, Context con)
                    throws IOException, InterruptedException {
                float soma_commidittes = 0.0f;
                int soma_repeat = 0;
                for (ReaptandSomaWritable v : values) {
                    soma_commidittes += v.getSoma();
                    soma_repeat += v.getReapt();
                }
                float media = soma_commidittes / soma_repeat;
                con.write(key, new FloatWritable(media));

            }
        }
    }

