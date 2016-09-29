/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.mapRed;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author Rodrigo B
 */
public class Personaje {

    // poiciones estructura toString
    public static final int POSICION_ID = 0;
    public static final int POSICION_PAIS = 1;
    public static final int POSICION_FECHA = 2;
    public static final int POSICION_RELACIONADOS = 3;

    private String id;
    private int idJSON;
    private String nombre;
    private Date fecha_nacimiento;
    private String pais_nacimiento;
    private String coverURL;
    private String url;

    private List<String> nombresRelaciones=new ArrayList<>();
    private List<Personaje> relacionados = new ArrayList<>();
    private List<Personaje> relacionadosFullData = new ArrayList<>();

    private static final SimpleDateFormat sdf=new SimpleDateFormat("yyyy/MM/dd");
    
    public Personaje(String texto) throws Exception{
        //?tefan Luchian	|2475912|Unknown|1868/02/01|Edgar Degas
        //texto=texto.replaceAll(";\t1", "");
        String[] datos=texto.split("\\|");
        this.id=datos[1];
        this.nombre=datos[0].trim();
        this.pais_nacimiento=datos[2];
        this.fecha_nacimiento=sdf.parse(datos[3]);
        this.coverURL = "https://en.wikipedia.org/w/api.php?action=query&pageids=" + id + "&prop=pageimages&format=json&pithumbsize=100";
        this.url = "https://en.wikipedia.org/?curid=" + id;
        //Personaje|1002676|Jerry Sadowitz|Unknown|1961/11/04|Lenny Bruce;Peter Cook;	1
        if(datos.length>4 && datos[4]!=null && datos[4].compareTo("")!=0){
            String[] nombresRelaciones=datos[4].split(";");
            for(int i=0;i<nombresRelaciones.length;i++){
                this.nombresRelaciones.add(nombresRelaciones[i]);
            }
        }
    }
    
    public Personaje(String id, String nombre, Date fecha_nacimiento, String pais_nacimiento) {
        this.id = id;
        this.nombre = nombre;
        this.fecha_nacimiento = fecha_nacimiento;
        this.pais_nacimiento = pais_nacimiento;
        this.coverURL = "https://en.wikipedia.org/w/api.php?action=query&pageids=" + id + "&prop=pageimages&format=json&pithumbsize=100";
        this.url = "https://en.wikipedia.org/?curid=" + id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public Date getFecha_nacimiento() {
        return fecha_nacimiento;
    }

    public void setFecha_nacimiento(Date fecha_nacimiento) {
        this.fecha_nacimiento = fecha_nacimiento;
    }

    public String getPais_nacimiento() {
        return pais_nacimiento;
    }

    public void setPais_nacimiento(String pais_nacimiento) {
        this.pais_nacimiento = pais_nacimiento;
    }

    public String getCoverURL() {
        return coverURL;
    }

    public void setCoverURL(String coverURL) {
        this.coverURL = coverURL;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<Personaje> getRelacionados() {
        return relacionados;
    }

    public void addRelacionado(Personaje personaje) {
        this.relacionados.add(personaje);
    }
    
    public List<Personaje> getRelacionadosFullData() {
        return relacionadosFullData;
    }
    
    public void addRelacionadoFullData(Personaje personaje) {
        this.relacionadosFullData.add(personaje);
    }

    public int getIdJSON() {
        return idJSON;
    }

    public void setIdJSON(int idJSON) {
        this.idJSON = idJSON;
    }

    public String getFecha_nacimientoString() {
        return sdf.format(fecha_nacimiento);
    }

    /**
     *
     * @param sFecha fecha en AAAA/MM/DD donde / pued cambiar por otro separador
     * según separador
     * @param separador separador de los datos de fecha
     * @param retornaHoySiNoEsFecha indica si quiere obtener la fecha actual si
     * no llega a ser fecha
     * @return
     */
    public static final Date getFechaDeString(String sFecha, String separador, boolean retornaHoySiNoEsFecha) {

        Date fecha = null;

        try {
            String[] split = sFecha.split(separador);
            Calendar c = Calendar.getInstance();
            c.set(Integer.parseInt(split[0]), Integer.parseInt(split[1]) - 1, Integer.parseInt(split[2]), 0, 0);
            fecha = c.getTime();
        } catch (Exception ex) {
            if (retornaHoySiNoEsFecha) {
                fecha = Calendar.getInstance().getTime();
            }
            // Si no, deja la fecha en null
        }

        return fecha;
    }

    public List<String> getNombresRelaciones() {
        return nombresRelaciones;
    }

    public void setNombresRelaciones(List<String> nombresRelaciones) {
        this.nombresRelaciones = nombresRelaciones;
    }

    @Override
    public String toString() {
        
        String sRelacionados = "";
        for (Personaje relacionado : relacionados) {
            sRelacionados += (sRelacionados.isEmpty() ? "" : ";") + relacionado.getNombre();
        }

        return  (id == null ? "" : id)
                + "|" + (pais_nacimiento == null ? "" : pais_nacimiento)
                + "|" + (fecha_nacimiento == null ? "" : getFecha_nacimientoString())
                + "|" + sRelacionados;
    }

    public String toGraphStringNodes() {

        String idsRelacionados = "";
        
        for (Personaje relacionado : relacionadosFullData) {
            idsRelacionados += (idsRelacionados.isEmpty() ? "" : ",") + "\n      " + relacionado.getIdJSON();
        }

        if (idsRelacionados.isEmpty()) {
            idsRelacionados += "\n";
        }

        return "{\n"
                + "   \"index\": " + idJSON + ", \n"
                + "   \"links\": [" + idsRelacionados + "   ], \n"
                + "   \"score\": 5, \n"
                + "   \"level\": 1, \n"
                + "   \"name\": \"" + nombre + "\", \n"
                + "   \"label\": \"" + nombre + "\", \n"
                + "   \"cover\": \"" + (coverURL == null ? "-" : coverURL) + "\", \n"
                + "   \"country\": \"" + pais_nacimiento + "\", \n"
                + "   \"birth_date\": \"" + fecha_nacimiento + "\", \n"
                + "   \"description\": \"xxxxx\", \n"
                + "   \"url\": \"" + (url == null ? "-" : url) + "\", \n"
                + "   \"id\": " + id + "\n"
                + "}";
    }

    public String toGraphStringLinks() {

        String stringLinks = "";

        for (Personaje relacionado : relacionadosFullData) {
            stringLinks += (stringLinks.isEmpty() ? "" : ",")
                    + "{"
                    + "\n   \"source\": " + this.getIdJSON()+ ","
                    + "\n   \"target\": " + relacionado.getIdJSON()+ ","
                    + "\n   \"weight\": 0.1"
                    + "\n}";
        }

        if (stringLinks.isEmpty()) {
            stringLinks += "\n";
        }

        return stringLinks;
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
