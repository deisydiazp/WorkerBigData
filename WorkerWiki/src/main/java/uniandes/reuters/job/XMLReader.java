package uniandes.reuters.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uniandes.mapRed.WCMapper;

public class XMLReader {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Se necesitan las carpetas de entrada y salida");
            System.exit(-1);
        }
        String entrada = args[0]; //carpeta de entrada
        String salida = args[1];//La carpeta de salida no puede existir

        try {
            ejecutarJob(entrada, salida);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void ejecutarJob(String entrada, String salida) throws IOException, ClassNotFoundException, InterruptedException {
        
        Configuration conf = new Configuration();
        
        conf.set("textinputformat.record.delimiter", "</page>");

        Job wcJob = Job.getInstance(conf, "Wiki Job");
        wcJob.setJarByClass(FilterReader.class);

        //Mapper
        wcJob.setMapperClass(WCMapper.class);

        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(IntWritable.class);
        wcJob.setNumReduceTasks(0);
        
        //Input Format
        TextInputFormat.setInputPaths(wcJob, new Path(entrada));
        wcJob.setInputFormatClass(TextInputFormat.class);

        ///Output Format
        TextOutputFormat.setOutputPath(wcJob, new Path(salida));
        wcJob.setOutputFormatClass(TextOutputFormat.class);
        
        wcJob.waitForCompletion(true);
        
    }
}
