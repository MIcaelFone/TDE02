package Tde.AvagereOfCommodityPerYear;
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
    //country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;quantity_name;quantity;category
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
// arquivo de entrada
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
// arquivo de saida
        Path output = new Path("output/AvagereOfCommodityPerYear");
// criacao do job e seu nome
        Job j = new Job(c, "AvagereOfCommodityPerYear");
// Registro de classes
        j.setJarByClass(AverageOfCommodityValuesPerYear.class); // Classe que tem o main
        j.setMapperClass(MapForAverageOfCommodityValuesPerYear.class); // Classe do MAP
        j.setReducerClass(ReduceAvangereOfCommodityValuesPerYear.class); // Classe do Reduce
        j.setCombinerClass(CombinerAverageofCommodityValuesPerYear.class);
// Tipos de saída das classes
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
    public static class MapForAverageOfCommodityValuesPerYear extends Mapper<LongWritable, Text, Text, ReaptandSomaWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] parts = linha.split(";");

            String Year = parts[1];
            String price = parts[5];
            if (parts.length < 6) {
                // The line does not have enough fields, skip it
                return;
            }

            if (!price.equals("trade_usd")) {
                try {
                    long preco = Long.parseLong(price);
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
            long soma_valores = 0;
            for (ReaptandSomaWritable v : values) {
                soma_repeat += v.getReapt();
                soma_valores += v.getCusto();

            }
            con.write(key, new ReaptandSomaWritable(soma_repeat,soma_valores));

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

    public static class ReduceAvangereOfCommodityValuesPerYear extends Reducer<Text, ReaptandSomaWritable, Text,LongWritable> {
        public void reduce(Text key, Iterable<ReaptandSomaWritable> values, Context con)
                throws IOException, InterruptedException {
            long soma_valores = 0;
            int soma_repeat = 0;
            for (ReaptandSomaWritable v : values) {
                soma_valores += v.getCusto();
                soma_repeat += v.getReapt();
            }
            long media = soma_valores / soma_repeat;

            con.write(key, new LongWritable(media));
        }
    }

}



