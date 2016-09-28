/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.reuters.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import uniandes.mapRed.Personaje;

/**
 *
 * @author Daniel
 */
public class Test2 {

    public static final String RUTA_ARCHIVO = "C:\\prueba\\part-r-00000";
    private static Map<String, Personaje> mapa = new HashMap<>();

    public void procesarArchivo() {
        if(mapa.size()!=0){
            return ;
        }
        File archivo = null;
        FileReader fr = null;
        BufferedReader br = null;

        try {
            archivo = new File(RUTA_ARCHIVO);
            fr = new FileReader(archivo);
            br = new BufferedReader(fr);

            // Lectura del fichero
            String linea;
            while ((linea = br.readLine()) != null) {
                Personaje p = new Personaje(linea);
                mapa.put(p.getNombre(), p);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // En el finally cerramos el fichero, para asegurarnos
            // que se cierra tanto si todo va bien como si salta 
            // una excepcion.
            try {
                if (null != fr) {
                    fr.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    public String toGraphStringNodes(Personaje p, Integer index, List<Integer> relaciones) {
        StringBuilder sb = new StringBuilder(" ");
        for (Integer i : relaciones) {
            sb.append(i).append(",");
        }
        String rels = sb.toString();

        return "{\n"
                + "   \"index\": " + index + ", \n"
                + "   \"links\": [" + rels.substring(0, rels.length() - 1) + "], \n"
                + "   \"score\": 7.5, \n"
                + "   \"level\": 1, \n"
                + "   \"name\": \"" + p.getNombre() + "\", \n"
                + "   \"label\": \"" + p.getNombre() + "\", \n"
                + "   \"cover\": \"" + (p.getCoverURL() == null ? "-" : p.getCoverURL()) + "\", \n"
                + "   \"country\": \"" + p.getPais_nacimiento() + "\", \n"
                + "   \"birth_date\": \"" + p.getFecha_nacimientoString() + "\", \n"
                + "   \"description\": \"xxxxx\", \n"
                + "   \"url\": \"" + (p.getUrl() == null ? "-" : p.getUrl()) + "\", \n"
                + "   \"id\": " + p.getId()
                + "\n},";
    }

    public String toGraphStringLinks(Integer indexA, Integer indexB) {

        String stringLinks =  "{"
                + "\n   \"source\": " + indexB + ","
                + "\n   \"target\": " + indexA + ","
                + "\n   \"weight\": 0.1"
                + "\n},";

        return stringLinks;
    }
    
    public static void main(String[] args) {
        System.out.println("1. " + System.currentTimeMillis());
        Test2 t = new Test2();
        t.procesarArchivo();
        System.out.println("2. " + System.currentTimeMillis());
        System.out.println("cant: " + mapa.size());
        Calendar c1 = Calendar.getInstance();
        c1.set(1968, 0, 1);
        Calendar c2 = Calendar.getInstance();
        c2.set(1979, 0, 1);
        Map<String, Personaje> filtro = new HashMap<>();
        Map<String, Integer> indices = new HashMap<>();
        int i = 0;
        for (String key : mapa.keySet()) {
            Personaje p = mapa.get(key);
            if (p.getFecha_nacimiento().after(c1.getTime()) && p.getFecha_nacimiento().before(c2.getTime())) {
                filtro.put(key, p);
                indices.put(key, i++);
                for (String rel : p.getNombresRelaciones()) {
                    if (!filtro.containsKey(rel)) {
                        Personaje p1 = mapa.get(rel);
                        if (p1 != null) {
                            filtro.put(rel, p1);
                            indices.put(rel, i++);
                        }
                    }
                }
            }
        }
        System.out.println("3. " + System.currentTimeMillis());
        System.out.println("cant: " + filtro.size());
        StringBuilder sbNodes = new StringBuilder();
        StringBuilder sbLinks = new StringBuilder();
        sbNodes.append("{\"nodes\":[");
        //sbLinks.append();
        for (String key : indices.keySet()) {
            List<Integer> rels = new ArrayList<>();
            Personaje p = filtro.get(key);
            Integer indexA=indices.get(key);
            for (String rel : p.getNombresRelaciones()) {
                Integer indexB = indices.get(rel);
                if (indexB != null) {
                    rels.add(indexB);
                    sbLinks.append(t.toGraphStringLinks(indexA, indexB));
                }
            }
            sbNodes.append(t.toGraphStringNodes(p, indexA, rels));
        }
        String texto = sbNodes.toString();
        texto = texto.substring(0, texto.length() - 1);
        texto = texto + "]";
        texto = texto + ",";
        texto = texto + "\"links\":[";
        texto = texto + sbLinks.toString();
        
        if(sbLinks.toString() != "")
            texto = texto.substring(0, texto.length() - 1);
        
        texto = texto + "]}";
        
        System.out.println("4. " + System.currentTimeMillis());
        System.out.println("datos: " + texto);
        System.out.println("5. " + System.currentTimeMillis());

    }

}
