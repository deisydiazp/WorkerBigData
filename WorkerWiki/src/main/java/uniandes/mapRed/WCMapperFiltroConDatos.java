package uniandes.mapRed;

import java.io.IOException;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapperFiltroConDatos extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final Log log = LogFactory.getLog(WCMapperFiltroConDatos.class);

    // archivo de cache
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // cache
    }

    @Override
    /**
     * Recibe el archivo de salida del WCMapper con todos los personajes de wiki
     * y hace join con archivo caché de personajes filtrados
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        context.write(new Text(context.toString()), new IntWritable(1));
        
        /*
        String[] datosPersonaje = value.toString().split("\\|");

        // obtiene fecha
        Date fecha_nacido = Personaje.getFechaDeString(datosPersonaje[Personaje.POSICION_FECHA], "/", false);

        // crea personaje
        Personaje personajeEncontrado = new Personaje(datosPersonaje[Personaje.POSICION_ID], datosPersonaje[Personaje.POSICION_NOMBRE], fecha_nacido, datosPersonaje[Personaje.POSICION_PAIS]);

        // obtiene los relacionados
        String[] relacionados = datosPersonaje[Personaje.POSICION_RELACIONADOS].split(";");
        int cantidadRelacionados = relacionados.length;

        while (cantidadRelacionados > 0) {
            cantidadRelacionados--;
            Personaje personajeRelacionado = new Personaje(null, relacionados[cantidadRelacionados], null, null);
            personajeEncontrado.addRelacionado(personajeRelacionado);
        }
        
        context.write(new Text(personajeEncontrado.toString()), new IntWritable(1));
        */
    }

}
