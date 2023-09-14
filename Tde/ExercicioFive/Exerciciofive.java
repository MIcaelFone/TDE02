package Tde.ExercicioFive;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class Exerciciofive {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/transactions_amostra.csv");
        // arquivo de saida
        Path output = new Path("tde.output/Exerciciofive");
        // criacao do job e seu nome
        Job j = new Job(c, "Exerciciofive");
// Registro de classes
        j.setJarByClass(Exerciciofive.class); // Classe que tem o main
        j.setMapperClass(MapForTransitionTypeFlowandPerYear.class); // Classe do MAP
        j.setReducerClass(ReduceExerciciofive.class); // Classe do Reduce
        //Tipos de saida
        j.setMapOutputKeyClass(ExercicioFiveKeyWritable.class);
        j.setMapOutputValueClass(ExercicioFiveValuesWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        j.setCombinerClass(CombinerExerciciofive.class);
        //Definição de arquivo de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(false);


    }

    public static class MapForTransitionTypeFlowandPerYear extends Mapper<LongWritable, Text, ExercicioFiveKeyWritable, ExercicioFiveValuesWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String Year = linha.split(";")[1];
            String unit = linha.split(";")[7];
            String price = linha.split(";")[5];
            int n = 1;
            if (!price.equals("trade_usd") && !Year.equals("year") && !unit.equals("quantity_name")) {

                try {
                    double preco = Double.parseDouble(price);

                    con.write(new ExercicioFiveKeyWritable(Year,unit), new ExercicioFiveValuesWritable(preco, n, 0, 0));
                } catch (NumberFormatException e) {


                }
            }
        }
    }


    public static class CombinerExerciciofive extends Reducer<ExercicioFiveKeyWritable, ExercicioFiveValuesWritable, ExercicioFiveKeyWritable,
            ExercicioFiveValuesWritable> {
        public void reduce(ExercicioFiveKeyWritable key, Iterable<ExercicioFiveValuesWritable> values, Context con)
                throws IOException, InterruptedException {
            double soma=0;
            int repeticao=0;
            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;


            for (ExercicioFiveValuesWritable v : values) {
                double num = v.getSoma();
                max= Math.max(max, num);
                min= Math.min(min, num);
                soma+=num;
                repeticao+=v.getCount();


            }




            con.write(new ExercicioFiveKeyWritable(key.getYear(), key.getUnit()), new ExercicioFiveValuesWritable(soma, repeticao,max,min));
        }
    }

    public static class ReduceExerciciofive extends Reducer<ExercicioFiveKeyWritable, ExercicioFiveValuesWritable, Text,
            Text> {
        public void reduce(ExercicioFiveKeyWritable key, Iterable<ExercicioFiveValuesWritable> values, Context con)
                    throws IOException, InterruptedException {
                double soma = 0;
                int repeticao=0;
                double max = Double.MIN_VALUE; // Inicializar max com o menor valor possível
                double min = Double.MAX_VALUE; //
                for (ExercicioFiveValuesWritable v : values) {
                    double num = v.getSoma();
                    max= Math.max(max, num);
                    min= Math.min(min, num);
                    soma+=num;
                    repeticao+=v.getCount();

                }

                double media = soma / repeticao;
                con.write(new Text(key.getUnit()+"  "+key.getYear()+"  "), new Text(" Media: "+media+"  Max: "+max+"  Min:  "+min));
            }
        }
    }

