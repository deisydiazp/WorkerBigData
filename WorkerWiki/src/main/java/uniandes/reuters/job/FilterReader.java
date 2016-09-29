package uniandes.reuters.job;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uniandes.mapRed.WCMapperFiltro;
import uniandes.mapRed.WCReducerJoin;

public class FilterReader {

    private static final String INTERMIDIATE_PATH = "/user/bigdata7/intermedio";// + Calendar.getInstance().getTime().toString();

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
         * Job 1 -  Filtra los personajes del archivo global de personajes de wikipedia
         *          Los personajes relacionados solo quedan con el nombre sin el resto de la data
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
        wcfJob.setMapOutputValueClass(Text.class);

        //wcfJob.setNumReduceTasks(0);

        //Input Format
        TextInputFormat.setInputPaths(wcfJob, new Path(entrada));
        wcfJob.setInputFormatClass(TextInputFormat.class);

        ///Output Format
        TextOutputFormat.setOutputPath(wcfJob, new Path(INTERMIDIATE_PATH));
        wcfJob.setOutputFormatClass(TextOutputFormat.class);

        wcfJob.waitForCompletion(true);

        /*
         * Job 2 - Reaiza un join de los personajes filtrados con los personajes del archivo global para obtener toda la data de los personajes
         */
        Job finalJob = Job.getInstance(conf, "Final Job");
        finalJob.setJarByClass(FilterReader.class);

        //Multiple Inputs and join reducer
        MultipleInputs.addInputPath(finalJob, new Path(entrada), TextInputFormat.class);
        MultipleInputs.addInputPath(finalJob, new Path(INTERMIDIATE_PATH), TextInputFormat.class);
        finalJob.setReducerClass(WCReducerJoin.class);
        
        // Output Format
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(finalJob, new Path(salida));
        finalJob.setOutputFormatClass(TextOutputFormat.class);

        finalJob.waitForCompletion(true);
    }
}
