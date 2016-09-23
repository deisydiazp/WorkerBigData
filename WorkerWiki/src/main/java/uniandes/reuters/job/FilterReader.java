package uniandes.reuters.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uniandes.mapRed.WCMapperFiltro;
import uniandes.mapRed.WCMapperFiltroConDatos;

public class FilterReader {
    
    private static final String INTERMIDIATE_PATH = "intermediate_output";
    
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Se necesitan las carpetas de entrada y salida; y parámetros: fecha inicial (YYYY/MM/DD), fecha final, pais y nombre");
            System.exit(-1);
        }

        String entrada = args[0];   //carpeta de entrada
        String salida = args[1];    //La carpeta de salida no puede existir
        // Parámetros
        String fechaIni = args[2];
        String fechaFin = args[3];
        String pais = args[4];
        String nombre = args[5];

        try {
            ejecutarJob(entrada, salida, fechaIni, fechaFin, pais, nombre);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void ejecutarJob(String entrada, String salida, String fechaIni, String fechaFin, String pais, String nombre) throws IOException, ClassNotFoundException, InterruptedException {

        /*
         * Job 1
         */
        Configuration conf = new Configuration();
        
        conf.set("fechaIni", fechaIni);
        conf.set("fechaFin", fechaFin);
        conf.set("pais", pais);
        conf.set("nombre", nombre);
        
        Job wcfJob = Job.getInstance(conf, "Filtro Job");
        wcfJob.setJarByClass(FilterReader.class);

        //Mapper
        wcfJob.setMapperClass(WCMapperFiltro.class);
        wcfJob.setMapOutputKeyClass(Text.class);
        wcfJob.setMapOutputValueClass(IntWritable.class);

        wcfJob.setNumReduceTasks(0);

        //Input Format
        TextInputFormat.setInputPaths(wcfJob, new Path(entrada));
        wcfJob.setInputFormatClass(TextInputFormat.class);

        ///Output Format
        TextOutputFormat.setOutputPath(wcfJob, new Path(INTERMIDIATE_PATH));
        wcfJob.setOutputFormatClass(TextOutputFormat.class);
        
        wcfJob.waitForCompletion(true);

        /*
         * Job 2
         */
        
        Job finalJob = Job.getInstance(conf, "Filtro Job");
        finalJob.setJarByClass(FilterReader.class);

        //Mapper
        finalJob.setMapperClass(WCMapperFiltroConDatos.class);
        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(IntWritable.class);

        finalJob.setNumReduceTasks(0);

        //Input Format
        TextInputFormat.setInputPaths(finalJob, new Path(INTERMIDIATE_PATH));
        finalJob.setInputFormatClass(TextInputFormat.class);

        ///Output Format
        TextOutputFormat.setOutputPath(finalJob, new Path(salida));
        finalJob.setOutputFormatClass(TextOutputFormat.class);
        finalJob.waitForCompletion(true);
    }
}
