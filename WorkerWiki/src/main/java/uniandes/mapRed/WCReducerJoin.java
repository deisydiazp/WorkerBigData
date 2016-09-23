package uniandes.mapRed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducerJoin extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String datosFinales = "";

        int conrtadorPersonajes = 0;
        for (Text value : values) {
            String datosValidar = value.toString();

            // Si encuentra un string con más datos se asume que es el que tiene mayor información
            if (datosValidar.length() > datosFinales.length()) {
                datosFinales = datosValidar;
            }
            conrtadorPersonajes++;
        }
        
        // Solo escribe si encontró el personaje al menos 2 veces: 1 en el archivo original y otra en el filtrado
        if (conrtadorPersonajes > 1) {
            context.write(key, new Text(datosFinales));
        }

    }

}
