/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.reuters.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import uniandes.mapRed.Personaje;

/**
 *
 * @author Rodrigo B
 */
public class Test {

    private final List<Personaje> personajes = new ArrayList();
    private final List<Personaje> personajesFiltrados = new ArrayList();

    public static final String RUTA_ARCHIVO = "C:\\prueba\\part-r-00000";
    public static final int POSICION_ID = 1;
    public static final int POSICION_NOMBRE = 2;
    public static final int POSICION_PAIS = 3;
    public static final int POSICION_FECHA = 4;
    public static final int POSICION_RELACIONADOS = 5;

    /**
     * Lee todos los personajes y los incluye a una lista global
     *
     * @param archivo archivo que tiene lista de personajes según salida del
     * map-reduce
     * @return exito o no de la operación si se leen personajes correctamente
     * @throws IOException
     */
    public boolean cargarPersonajesEsExitoso(File archivo) throws IOException {

        personajes.clear();

        try (BufferedReader br = new BufferedReader(new FileReader(archivo))) {
            String line = br.readLine();
            
            while (line != null) {
                String[] datosPersonaje = line.split("\\|");

                // obtiene fecha
                Date fecha_nacido = Personaje.getFechaDeString(datosPersonaje[POSICION_FECHA], "/", false);
                
                // crea personaje
                Personaje personajeEncontrado = new Personaje(datosPersonaje[POSICION_ID], datosPersonaje[POSICION_NOMBRE], fecha_nacido, datosPersonaje[POSICION_PAIS]);

                // obtiene los relacionados
                String[] relacionados = datosPersonaje[POSICION_RELACIONADOS].split(";");
                int cantidadRelacionados = relacionados.length;
                while (cantidadRelacionados > 0) {
                    cantidadRelacionados--;
                    Personaje personajeRelacionado = new Personaje(null, relacionados[cantidadRelacionados], null, null);
                    personajeEncontrado.addRelacionado(personajeRelacionado);
                }
                
                personajes.add(personajeEncontrado);
                
                line = br.readLine();
                System.out.println("lineas: " + line);
            }
        }

        return !personajes.isEmpty();
    }

    public List<Personaje> obtenerPersonajesPorFiltro(File archivo, Date fechaIni, Date fechaFin, String pais, String nombre) throws IOException {

        if (cargarPersonajesEsExitoso(archivo)) {

            personajesFiltrados.clear();

            // filtra los personajes según los parámetros
            Iterator<Personaje> itPersonajes = personajes.iterator();
            while (itPersonajes.hasNext()) {
                Personaje next = itPersonajes.next();

                // Validación de formatos de filtros
                Date fecha_nacido = next.getFecha_nacimiento();
                
                if (pais == null) {
                    pais = "";
                }
                if (nombre == null) {
                    nombre = "";
                }

                if ((nombre.isEmpty() || next.getNombre().contains(nombre))
                        && (fechaIni == null || fecha_nacido.after(fechaIni))
                        && (fechaFin == null || fecha_nacido.before(fechaFin))
                        && (pais.isEmpty() || next.getPais_nacimiento().contains(pais))) {
                    personajesFiltrados.add(next);
                }
            }
            
            int contadorIndexJSON = 0;
            for(Personaje filtrado : personajesFiltrados){
                for(Personaje relacionado : filtrado.getRelacionados()){
                    for(Personaje personajeGlobal : personajes){
                        // si encuentra el personaje por nombre en la lista global lo agrega con todos sus datos como relacionado
                        if (relacionado.getNombre().equals(personajeGlobal.getNombre())) {
                            filtrado.addRelacionadoFullData(personajeGlobal);
                        }
                    }
                }
                filtrado.setIdJSON(contadorIndexJSON);
                contadorIndexJSON++;
            }
            
        }
        
        for(Personaje per: personajesFiltrados){
            System.out.println(">" + per);
        }
        
        System.out.println(obtenerStringJSONPersonajes(personajesFiltrados));
        
        return personajesFiltrados;
    }
    
    public String obtenerStringJSONPersonajes(List<Personaje> personajes){
        
        String stringNodesJSON = "\n\"nodes\": [";
        String stringLinksJSON = "\n\"links\": [";
        
        int longitudInicialNodesJSON = stringNodesJSON.length();
        int longitudInicialLinksJSON = stringLinksJSON.length();
        
       Iterator<Personaje> itPersonajes = personajes.iterator();
        while (itPersonajes.hasNext()) {
            Personaje persona = itPersonajes.next();
            stringNodesJSON += (stringNodesJSON.length() == longitudInicialNodesJSON ? "\n" : ",\n") + persona.toGraphStringNodes();
            if (!persona.getRelacionadosFullData().isEmpty()) {
                stringLinksJSON += (stringLinksJSON.length() == longitudInicialLinksJSON ? "\n" : ",\n") + persona.toGraphStringLinks();
            }
        }
        
        stringNodesJSON += "\n],";
        stringLinksJSON += "\n]";
        
        String stringJSON = "{" + stringNodesJSON + stringLinksJSON + "\n}";
        
        return stringJSON;
    }    
    
    public static void main(String[] args) {
        
        Test test = new Test();
        try {
            test.obtenerPersonajesPorFiltro(new File(RUTA_ARCHIVO), null, null, null, null);
        } catch (IOException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
        }

        /*
            String sFecha = "1977|2|2";
            String[] split = sFecha.split("\\|");
            
            Calendar c = Calendar.getInstance();
            c.tring[] split = sFecha.split("\\|");
            set(Integer.parseInt(split[0]), Integer.parseInt(split[1]) - 1, Integer.parseInt(split[2]), 0, 0);
            Date fecha_nacido = c.getTime();
            
            System.out.println(fecha_nacido.toString());
            
            Personaje p = new Personaje("5", null, fecha_nacido, "COL");
            System.out.println("p: " + p.toString());
            
            p.addRelacionado(new Personaje("10", "Rodrigo", fecha_nacido, "ARG"));
            p.addRelacionado(new Personaje("12", "Juan", fecha_nacido, "MEX"));
            
            System.out.println(p.toGraphString());
            
            try {
            System.out.println(new Test().obtenerURLImagen("https://en.wikipedia.org/w/api.php?action=query&pageids=34802102&prop=pageimages&format=json&pithumbsize=100"));
            } catch (IOException ex) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            }*/
    }

    public String obtenerURLImagen(String personajeCoverURL) throws MalformedURLException, IOException {

        String urlImg = null;

        InputStream input = new URL(personajeCoverURL).openStream();
        String jsonString = IOUtils.toString(input);

        Pattern regexImg = Pattern.compile("http(.)*jpg\",\"width");
        Matcher mImg = regexImg.matcher(jsonString);

        if (mImg.find()) {
            urlImg = mImg.group(0);
            urlImg = urlImg.replaceAll("\",\"width", "");
        }

        return urlImg;
    }

}
