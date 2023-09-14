import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class ExercicioSixPartTwo {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path input = new Path("tde.output/ExercicioSix/part-r-00000");
        Path output = new Path("tde.output/ExercicioSixPartTwo");
        Job job = Job.getInstance(conf, "HighestPriceofCountryPartTwo");
        job.setJarByClass(ExercicioSixPartTwo.class); // Classe que tem o main
        job.setMapperClass(MapHighestPricePartTwo.class); // Classe do MAP
        job.setReducerClass(ReducerHigestPricePartTwo.class); // Classe do Reduce
        job.setMapOutputKeyClass(Text.class); // chave
        job.setMapOutputValueClass(DoubleWritable.class); // valor
        job.setOutputKeyClass(Text.class); // chave de saída
        job.setOutputValueClass(DoubleWritable.class); // valor de saída
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapHighestPricePartTwo extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] parts = linha.split("\t");

            String country = parts[0];
            String price = parts[1];

            if (!price.equals("trade_usd")) {
                try {
                    double preco = Double.parseDouble(price);
                    context.write(new Text(country), new DoubleWritable(preco));
                } catch (NumberFormatException e) {
                    // Handle the case where price cannot be parsed as a float
                    // You may log this error or handle it as needed
                }
            }
        }
    }
 // A lógica era de tentar pegar da lista de países com maior preço, o país com maior preço, mas não consegui fazer funcionar.
    public static class ReducerHigestPricePartTwo extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        double max = Double.NEGATIVE_INFINITY;
        String maiorPais = "";

        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                if (value.get() > max) {
                    max = value.get();
                    maiorPais = key.toString(); //Pegue o pais do maior valor
                }
            }
            con.write(new Text(maiorPais), new DoubleWritable(max));
        }
    }
}




