package uniandes.mapRed;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapperFiltro extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final Log log = LogFactory.getLog(WCMapperFiltro.class);
    
    // Filtros establecidos por usuario
    private Date fechaIni; // YYYY/MM/DD
    private Date fechaFin; // YYYY/MM/DD
    private String pais;
    private String nombre;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        fechaIni = Personaje.getFechaDeString(context.getConfiguration().get("fechaIni", ""), "/", false);
        fechaFin = Personaje.getFechaDeString(context.getConfiguration().get("fechaFin", ""), "/", false);
        pais = context.getConfiguration().get("pais", "");
        nombre = context.getConfiguration().get("nombre", "");
        
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] datosPersonaje = value.toString().split("\\|");

        // obtiene fecha
        Date fecha_nacido = Personaje.getFechaDeString(datosPersonaje[Personaje.POSICION_FECHA], "/", false);

        // crea personaje
        Personaje personajeEncontrado = new Personaje(datosPersonaje[Personaje.POSICION_ID], datosPersonaje[Personaje.POSICION_NOMBRE], fecha_nacido, datosPersonaje[Personaje.POSICION_PAIS]);

        if ((nombre.isEmpty() || personajeEncontrado.getNombre().contains(nombre))
                && (fechaIni == null || fecha_nacido.after(fechaIni))
                && (fechaFin == null || fecha_nacido.before(fechaFin))
                && (pais.isEmpty() || personajeEncontrado.getPais_nacimiento().contains(pais))) {

            // obtiene los relacionados
            String[] relacionados = datosPersonaje[Personaje.POSICION_RELACIONADOS].split(";");
            int cantidadRelacionados = relacionados.length;

            while (cantidadRelacionados > 0) {
                cantidadRelacionados--;
                Personaje personajeRelacionado = new Personaje(null, relacionados[cantidadRelacionados], null, null);
                personajeEncontrado.addRelacionado(personajeRelacionado);
                context.write(new Text(personajeEncontrado.toString()), new IntWritable(1));
            }
            
            context.write(new Text(personajeEncontrado.toString()), new IntWritable(1));

        }

    }

}
