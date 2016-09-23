/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uniandes.mapRed;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Rodrigo B
 */
public class Personaje {

    // poiciones estructura toString
    public static final int POSICION_ID = 1;
    public static final int POSICION_NOMBRE = 2;
    public static final int POSICION_PAIS = 3;
    public static final int POSICION_FECHA = 4;
    public static final int POSICION_RELACIONADOS = 5;

    private String id;
    private int idJSON;
    private String nombre;
    private Date fecha_nacimiento;
    private String pais_nacimiento;
    private String coverURL;
    private String url;

    private List<Personaje> relacionados = new ArrayList();
    private List<Personaje> relacionadosFullData = new ArrayList();

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
        String sFecha = "";

        if (fecha_nacimiento != null) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
            sFecha = dateFormat.format(fecha_nacimiento);
        }

        return sFecha;
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

    @Override
    public String toString() {
        
        String sRelacionados = "";
        for (Personaje relacionado : relacionados) {
            sRelacionados += (sRelacionados.isEmpty() ? "" : ";") + relacionado.getNombre();
        }

        return (id == null ? "" : id)
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

}
