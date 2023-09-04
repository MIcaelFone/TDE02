package Tde.CountTransictionBrazil;
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
// arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
// arquivo de saida
        Path output = new Path("output/CountTransictionBrazil");
// criacao do job e seu nome
        Job j = new Job(c, "CountTransictionBrazil");
// Registro de classes
        j.setJarByClass(CountTransictionBrazil.class); // Classe que tem o main
        j.setMapperClass(MapForTransitionBrazilCount.class); // Classe do MAP
        j.setReducerClass(MapForTransitionBrazilCount.ReduceTransitionBrazilCount.class); // Classe do Reduce
//j.setCombinerClass(Combiner.class); // Combiner
        j.setCombinerClass(MapForTransitionBrazilCount.ReduceTransitionBrazilCount.class); // Combiner igual ao Reduce!
// Definir tipos de saida
// Map
        j.setMapOutputKeyClass(Text.class); // chave
        j.setMapOutputValueClass(IntWritable.class); // valor
// Reduce
        j.setOutputKeyClass(Text.class); // chave
        j.setOutputValueClass(IntWritable.class); // valor
// Cadastrar arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
// Rodar :)
        j.waitForCompletion(false);
    }

    /**
     * Funcao de map eh chamada POR linha do arquivo de entrada
     * Dois tipos de entrada e dois tipos de saida
     * 1a info: tipo da chave de entrada (LONGWRITABLE -> NAO MEXER!)
     * 2a info: tipo do valor de entrada (Text -> linha do arquivo de entrada)
     * 3a info: tipo da chave de saida (Text -> palavra)
     * 4a info: tipo do valor de saida (Int -> 1)
     */
    public static class MapForTransitionBrazilCount extends Mapper<LongWritable, Text,
            Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
// value = linha de entrada
// obtendo a linha como string
            String country = value.toString();
// quebrando a linha em palavras
            String[] pais_requirido = country.split(";");
            if (Objects.equals(pais_requirido[0], "Brazil")) {
                con.write(new Text(pais_requirido[0]), new IntWritable(1));
            }
        }
        public static class Combiner extends Reducer<Text, IntWritable, Text,
                IntWritable>{
            public void reduce(Text key, Iterable<IntWritable> values, Context con)
                    throws IOException, InterruptedException {
// pré-somando valores em cada Mapper (computador que roda MAP)
                int soma = 0;
                for (IntWritable v : values){
                    soma += v.get();
                }
// passando pro reduce valores pré-somados
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

}





// loop pra criar tuplas (chave=p, valor=1)




