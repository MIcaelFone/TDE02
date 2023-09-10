package Tde.ExercicioFour;
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

public class Exerciciofour {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("in/transactions_amostra.csv");
        Path output = new Path("tde.output/Exerciciofour");
        Job j = new Job(c, "AvagereOfCommodityPerYear");
        j.setJarByClass(Exerciciofour.class); // Classe que tem o main
        j.setMapperClass(MapAveragePriceOfCommoditiesperunittypeYearandCategoryintheexportflowinBrazil.class); // Classe do MAP
        j.setReducerClass(ReduceAveragePriceOfCommoditiesperunittypeYearandCategoryintheexportflowinBrazil.class); // Classe do Reduce
        j.setCombinerClass(CombineAveragePriceOfCommoditiesperunittypeYearandCategoryintheexportflowinBrazil.class);
        j.setMapOutputKeyClass(ExerciciofourKeyWritable.class);
        j.setMapOutputValueClass(ExerciciofourValueWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(false);
    }


    public static class MapAveragePriceOfCommoditiesperunittypeYearandCategoryintheexportflowinBrazil extends Mapper<LongWritable, Text,
            ExerciciofourKeyWritable, ExerciciofourValueWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String linha = value.toString();
            String[] parts = linha.split(";");

            String Year = parts[1];
            String price = parts[5];
            String country = parts[0];
            String flow = parts[4];
            String unit = parts[7];
            String category = parts[9];
            int n=1;


            if (!price.equals("trade_usd")  && !category.equals("category") && !flow.equals("flow") && !unit.equals("quantity_name") && !Year.equals("year") && !country.equals("country_or_area")) {
                if(flow.equals("Export") && country.equals("Brazil")){
                    try {
                        double preco = Double.parseDouble(price);
                        con.write(new ExerciciofourKeyWritable(unit,Year,category), new ExerciciofourValueWritable(n, preco));
                    } catch (NumberFormatException e) {
                        // Handle the case where price cannot be parsed as a float
                        // You may log this error or handle it as needed
                    }

                }
            }
        }
    }






        public static class CombineAveragePriceOfCommoditiesperunittypeYearandCategoryintheexportflowinBrazil extends Reducer<ExerciciofourKeyWritable, ExerciciofourValueWritable, ExerciciofourKeyWritable,
                ExerciciofourValueWritable> {
            public void reduce(ExerciciofourKeyWritable key, Iterable<ExerciciofourValueWritable> values, Context con)
                    throws IOException, InterruptedException {
                int soma_repeat = 0;
                double soma_precos = 0;
                for (ExerciciofourValueWritable v : values) {
                    soma_repeat += v.getReapt();
                    soma_precos += v.getPreco();

                }

                con.write(key, new ExerciciofourValueWritable(soma_repeat,soma_precos));

            }
        }


        public static class ReduceAveragePriceOfCommoditiesperunittypeYearandCategoryintheexportflowinBrazil extends Reducer<ExerciciofourKeyWritable, ExerciciofourValueWritable, Text,
                DoubleWritable> {
            public void reduce(ExerciciofourKeyWritable key, Iterable<ExerciciofourValueWritable> values, Context con)
                    throws IOException, InterruptedException {
                int soma_repeat = 0;
                double soma_precos = 0;
                for (ExerciciofourValueWritable v : values) {
                    soma_repeat += v.getReapt();
                    soma_precos += v.getPreco();
                }

                double media = soma_precos / soma_repeat;
                con.write(new Text(key.getUnit()+"  "+key.getYear()+"  "+key.getCategory() +"  "), new DoubleWritable(media));
            }

        }


    }


