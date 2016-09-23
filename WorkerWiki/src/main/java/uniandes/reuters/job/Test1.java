/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.reuters.job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;
import uniandes.mapRed.Personaje;
import static uniandes.reuters.job.Test.POSICION_FECHA;

/**
 *
 * @author Rodrigo B
 */
public class Test1 {

    public static final String RUTA_ARCHIVO = "C:\\prueba\\part-r-00000_0";
    public static final int POSICION_ID = 1;
    public static final int POSICION_NOMBRE = 2;
    public static final int POSICION_PAIS = 3;
    public static final int POSICION_FECHA = 4;
    public static final int POSICION_RELACIONADOS = 5;

    public static void AsignarId(File archivo, Date fechaInicio, Date fechaFin){
        
        HashMap<String,Integer> personas = new HashMap <String,Integer>();
        String line = "";
        try (BufferedReader br = new BufferedReader(new FileReader(archivo))) {
                line = br.readLine();
                while (line != null) {
                    String[] datosPersonaje = line.replace(";\t1","").split("\\|");
                    Date fechaNacido = Personaje.getFechaDeString(datosPersonaje[POSICION_FECHA], "/", false);
                    
                    if (fechaNacido.after(fechaInicio) && fechaNacido.before(fechaFin)) {
                        
                        if(!personas.containsKey(datosPersonaje[POSICION_NOMBRE])){
                            int id = personas.size()+ 1;
                            personas.put(datosPersonaje[POSICION_NOMBRE], id);
                        }
                        
                        if(datosPersonaje.length > 5){
                            String[] relacionados = datosPersonaje[POSICION_RELACIONADOS].split(";");
                            for(String nombreRelacion: relacionados){
                                int id = personas.size()+ 1;
                                if(!personas.containsKey(nombreRelacion)){
                                    personas.put(nombreRelacion, id);
                                }
                            }
                        }
                    }
                    line = br.readLine();
                }
                
                br.close();
                CrearArchivo(personas, archivo);
                
        }catch(Exception ex){
            System.out.println("Error: CreandoID" + ex.toString() + "******** " + line);
        }
    }

    private static void CrearArchivo(HashMap<String, Integer> personas, File archivo) {
        
        try (BufferedReader br = new BufferedReader(new FileReader(archivo))) {
            
            String line = br.readLine();
            String stringNodos = "";
            String stringLinks = "";
            while (line != null) {
                
                String[] datosPersonaje = line.replace(";\t1","").split("\\|");

                String nombrePersona = datosPersonaje[POSICION_NOMBRE];
                if(!personas.containsKey(nombrePersona)){
                    line = br.readLine();
                    continue;
                }
                
                String cadRelacionados ="";
                if(datosPersonaje.length > 5){
                    String[] relacionados = datosPersonaje[POSICION_RELACIONADOS].split(";");
                    for(String nombreRelacion: relacionados){
                        if(nombreRelacion!="")
                            cadRelacionados += "," + personas.get(nombreRelacion);
                        
                            stringLinks += (stringLinks.isEmpty() ? "" : ",")
                                    + "{"
                                    + "\n   \"source\": " + personas.get(nombreRelacion)+ ","
                                    + "\n   \"target\": " + personas.get(nombrePersona)+ ","
                                    + "\n   \"weight\": 0.1"
                                    + "\n}";
                    }
                    cadRelacionados = cadRelacionados.substring(1,cadRelacionados.length());
                }
                
                stringNodos   += (stringNodos.isEmpty() ? "\n{" : ",\n{")
                        + "   \"index\": " + personas.get(nombrePersona) + ", \n"
                        + "   \"links\": [" + cadRelacionados + "], \n"
                        + "   \"score\": 7.5, \n"
                        + "   \"level\": 1, \n"
                        + "   \"name\": \"" + datosPersonaje[POSICION_NOMBRE] + "\", \n"
                        + "   \"label\": \"" + datosPersonaje[POSICION_NOMBRE] + "\", \n"
                        + "   \"cover\": \"" + "https://en.wikipedia.org/w/api.php?action=query&pageids=" + datosPersonaje[POSICION_ID] + "&prop=pageimages&format=json&pithumbsize=100" + "\", \n"
                        + "   \"country\": \"" + datosPersonaje[POSICION_PAIS] + "\", \n"
                        + "   \"birth_date\": \"" + datosPersonaje[POSICION_FECHA] + "\", \n"
                        + "   \"description\": \"xxxxx\", \n"
                        + "   \"url\": \"" + "https://en.wikipedia.org/?curid=" + datosPersonaje[POSICION_ID] + "\", \n"
                        + "   \"id\": " + datosPersonaje[POSICION_ID] + "\n"
                        + "}";
               
                line = br.readLine();
            }   
            br.close();

            FileWriter fw = new FileWriter("C:\\prueba\\part.txt");
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write("{\n\"nodes\": [" + stringNodos + "],\n\"links\": [\n" + stringLinks + "\n]\n}");
            bw.close();
            
        }catch(Exception ex){
            System.out.println("Error: CreandoJson" + ex.toString());
        }
    
    }
    
     public static void main(String[] args) {
        
        //Test test = new Test();
        Calendar cal = Calendar.getInstance();
        cal.set(1968,11,20);
        Date fechaInicio = cal.getTime(); 
        
        cal.set(2000,11,20);
        Date fechaFin = cal.getTime(); 
        
        AsignarId(new File(RUTA_ARCHIVO),fechaInicio,fechaFin);
        
     }
}

