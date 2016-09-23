package uniandes.mapRed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducerJoin extends Reducer<Text, Text, Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        String datosFinales = "";
        
        int i = 0;
        for (Text value : values) {
            String datosValidar = value.toString();
            
            // Si encuentra un string con más datos se asume que es el que tiene mayor información
            if (datosValidar.length() > datosFinales.length()){
                datosFinales = datosValidar;
            }
        }
        
        context.write(key, new Text(datosFinales));

    }

}
