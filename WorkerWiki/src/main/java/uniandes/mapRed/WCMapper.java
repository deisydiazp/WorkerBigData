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

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final Log log = LogFactory.getLog(WCMapper.class);

    //public static final String TIPOS_PERSONAS = "(person|fictional artifact|Buddha|Dalai Lama|Christian leader|Hindu leader|Jewish leader|Latter Day Saint biography|Muslim leader|rebbe|religious biography|saint|peerage title|baronetage|Egyptian dignitary|noble|peer|pharaoh|pretender|royalty|college football player|gridiron football person|NFL biography|baseball biography|MLB umpire|basketball biography|basketball official|Champ Car driver|F1 driver|Le Mans driver|Motocross rider|motorcycle rider|NASCAR driver|racing driver|racing driver series section|speedway rider|WRC driver|sportsperson|biathlete|boxer|speed skater|sailor|sport wrestler|swimmer|AFL biography|alpine ski racer|amateur wrestler|badminton player|bandy biography|bodybuilder|boxer|climber|cricketer|curler|cyclist|equestrian|fencer|field hockey player|figure skater|football biography|football official|GAA player|golfer|gymnast|handball biography|horseracing personality|ice hockey player|lacrosse player|martial artist|mountaineer|NCAA athlete|netball biography|NHL coach|pelotari|professional bowler|professional wrestler|rugby biography|rugby league biography|rugby union biography|Rugby Union biography|skier|sports announcer detailssquash player|sumo wrestler|surfer|table tennis player|tennis biography|volleyball biography|person|academic|adult biography|architect|clergy|dancer|fashion designer|medical details|medical person|Native American leader|sports announcer|theologian|theological work|artist|astronaut|aviator|bullfighting career|chef|chess biography|Chinese historical biography|Chinese|classical composer|college coach|comedian|comics creator|criminal|darts player|economist|engineer|engineering career|FBI Ten Most Wanted|go player|gunpowder plotter|Magic|member of the Knesset|military person|model|musical artist|Nahua officeholder|officeholder|pageant titleholder|philosopher|pirate|Playboy Playmate|playwright|poker player|police officer|presenter|Pro Gaming player|scholar|scientist|snooker player|spy|War on Terror detainee|writer|YouTube personality)";
    Pattern TIPOS_PERSONAS = Pattern.compile("(Buddha|Dalai Lama|Christian leader|Hindu leader|Jewish leader|Latter Day Saint biography|Muslim leader|rebbe|religious biography|saint|peerage title|baronetage|Egyptian dignitary|noble|peer|pharaoh|pretender|royalty|college football player|gridiron football person|NFL biography|baseball biography|MLB umpire|basketball biography|basketball official|Champ Car driver|F1 driver|Le Mans driver|Motocross rider|motorcycle rider|NASCAR driver|racing driver|racing driver series section|speedway rider|WRC driver|sportsperson|biathlete|boxer|speed skater|sailor|sport wrestler|swimmer|AFL biography|alpine ski racer|amateur wrestler|badminton player|bandy biography|bodybuilder|boxer|climber|cricketer|curler|cyclist|equestrian|fencer|field hockey player|figure skater|football biography|football official|GAA player|golfer|gymnast|handball biography|horseracing personality|ice hockey player|lacrosse player|martial artist|mountaineer|NCAA athlete|netball biography|NHL coach|pelotari|professional bowler|professional wrestler|rugby biography|rugby league biography|rugby union biography|Rugby Union biography|skier|sports announcer detailssquash player|sumo wrestler|surfer|table tennis player|tennis biography|volleyball biography|person|academic|adult biography|architect|clergy|dancer|fashion designer|medical details|medical person|Native American leader|sports announcer|theologian|theological work|artist|astronaut|aviator|bullfighting career|chef|chess biography|Chinese historical biography|Chinese|classical composer|college coach|comedian|comics creator|criminal|darts player|economist|engineer|engineering career|FBI Ten Most Wanted|go player|gunpowder plotter|Magic|member of the Knesset|military person|model|musical artist|Nahua officeholder|officeholder|pageant titleholder|philosopher|pirate|Playboy Playmate|playwright|poker player|police officer|presenter|Pro Gaming player|scholar|scientist|snooker player|spy|War on Terror detainee|writer|YouTube personality)");
    public static final String TIPOS_RELACIONES = "influences|influenced|associated_acts";
    public static final String PAISES = "(Afghanistan|Albania|Algeria|Andorra|Angola|Antigua & Deps|Argentina|Armenia|Australia|Austria|Azerbaijan|Bahamas|Bahrain|Bangladesh|Barbados|Belarus|Belgium|Belize|Benin|Bhutan|Bolivia|Bosnia Herzegovina|Botswana|Brazil|Brunei|Bulgaria|Burkina|Burundi|Cambodia|Cameroon|Canada|Cape Verde|Central African Rep|Chad|Chile|China|Colombia|Comoros|Congo|Congo|Costa Rica|Croatia|Cuba|Cyprus|Czech Republic|Denmark|Djibouti|Dominica|Dominican Republic|East Timor|Ecuador|Egypt|El Salvador|Equatorial Guinea|Eritrea|Estonia|Ethiopia|Fiji|Finland|France|Gabon|Gambia|Georgia|Germany|Ghana|Greece|Grenada|Guatemala|Guinea|Guinea-Bissau|Guyana|Haiti|Honduras|Hungary|Iceland|India|Indonesia|Iran|Iraq|Ireland|Israel|Italy|Ivory Coast|Jamaica|Japan|Jordan|Kazakhstan|Kenya|Kiribati|Korea North|Korea South|Kosovo|Kuwait|Kyrgyzstan|Laos|Latvia|Lebanon|Lesotho|Liberia|Libya|Liechtenstein|Lithuania|Luxembourg|Macedonia|Madagascar|Malawi|Malaysia|Maldives|Mali|Malta|Marshall Islands|Mauritania|Mauritius|Mexico|Micronesia|Moldova|Monaco|Mongolia|Montenegro|Morocco|Mozambique|Myanmar|Namibia|Nauru|Nepal|Netherlands|New Zealand|Nicaragua|Niger|Nigeria|Norway|Oman|Pakistan|Palau|Panama|Papua New Guinea|Paraguay|Peru|Philippines|Poland|Portugal|Qatar|Romania|Russian Federation|Rwanda|St Kitts & Nevis|St Lucia|Saint Vincent & the Grenadines|Samoa|San Marino|Sao Tome & Principe|Saudi Arabia|Senegal|Serbia|Seychelles|Sierra Leone|Singapore|Slovakia|Slovenia|Solomon Islands|Somalia|South Africa|South Sudan|Spain|Sri Lanka|Sudan|Suriname|Swaziland|Sweden|Switzerland|Syria|Taiwan|Tajikistan|Tanzania|Thailand|Togo|Tonga|Trinidad & Tobago|Tunisia|Turkey|Turkmenistan|Tuvalu|Uganda|Ukraine|United Arab Emirates|United Kingdom|United States|Uruguay|Uzbekistan|Vanuatu|Vatican City|Venezuela|Vietnam|Yemen|Zambia|Zimbabwe)";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String xmlString = value.toString();

        try {

            Pattern regexItems = Pattern.compile("\\{Infobox [A-Za-z]+");

            Matcher mItems = regexItems.matcher(xmlString);

            if (mItems.find()) { // Si es un personaje obtiene información adicional

                String stringInfoBox = mItems.group(0);

                Matcher person = TIPOS_PERSONAS.matcher(stringInfoBox);
                if (person.find()) {

                    log.info("****PASO 1: es person ");

                    String title = "";
                    String id = "";
                    String string_fecha_nacido; // formato AAAA|MM|dd
                    Date fecha_nacido = Calendar.getInstance().getTime(); // para los que no encuentre fecha pone la fecha de hoy para poderlos buscar
                    String lugar_nacido = "";

                    // INFORMACIÓN ADICIONAL DEL XML
                    // Obtiene el TITLE del title (nombre del personje si JSON indica que es un personaje)
                    Pattern regexTITLE = Pattern.compile("(?:<title>)(.+)(?:<\\/title>)");
                    Matcher mTITLE = regexTITLE.matcher(xmlString);

                    if (mTITLE.find()) {
                        title = mTITLE.group(1);
                        log.info("****PASO 2: obtiene nombre");
                    }

                    // Obtiene el ID del title
                    Pattern regexID = Pattern.compile("(?:<id>)(.+)(?:<\\/id>)");
                    Matcher mID = regexID.matcher(xmlString);

                    if (mID.find()) {
                        id = mID.group(1);
                        log.info("****PASO 3: obtiene el id");
                    }

                    // INFORMACIÓN DE ESTRUCTURA WIKIPEDIA
                    // Obtiene fecha nacimiento
                    // Estructura similar a: birth_date = {{Birth date|df=yes|1818|05|05}}
                    Pattern regexNacido = Pattern.compile("\\|.*birth_date.*=*");

                    Matcher mNacido = regexNacido.matcher(xmlString);
                    if (mNacido.find()) {
                        string_fecha_nacido = mNacido.group(0);
                        
                        log.info("****PASO 4: obtiene la fecha de nacimiento: " + string_fecha_nacido);
                        
                        Pattern regexFecha = Pattern.compile("(\\d{4}\\|\\d{1,2}\\|\\d{1,2})");
                        Matcher mFecha = regexFecha.matcher(string_fecha_nacido);
                        if (mFecha.find()) {
                            fecha_nacido = Personaje.getFechaDeString(mFecha.group(0), "\\|", true);
                        }
                        
                        log.info("****PASO 5: arma la fecha de nacimiento: " + fecha_nacido.toString());
                    }

                    // Obtiene lugar de nacimiento - no tiene un patron general
                    // Estructura similar a: birth_place = [[Barranquilla]], [[Atlántico Department|Atlántico]], Colombia
                    Pattern regexPais = Pattern.compile("\\|(.)*birth_place.*");
                    Matcher mPais = regexPais.matcher(xmlString);

                    if (mPais.find()) {
                        lugar_nacido = mPais.group(0);
                        log.info("****PASO 6: obtiene el lugar de nacimiento: " + lugar_nacido);
                        Pattern regexPaisValido = Pattern.compile(PAISES);
                        Matcher mPaisValido = regexPaisValido.matcher(lugar_nacido);
                        if (mPaisValido.find()) {
                            lugar_nacido = mPaisValido.group(0);
                            log.info("****PASO 7: arma el pais de nacimiento: " + lugar_nacido);
                        }else{
                            lugar_nacido = "Unknown";
                        }   
                    }

                    Personaje personajeEncontrado = new Personaje(id, title, fecha_nacido, lugar_nacido);

                    //  Obtiene influencias y relacionados
                    //      Estructuras  con | influences       =  | influenced       = 
                    //      si se quiere validar flat lists \|(.)*(influences|influenced|associated_acts)(.|\n)*?(\{Endflatlist\})
                    Pattern regexInfluencias = Pattern.compile("\\|(.)*(" + TIPOS_RELACIONES + ")(.)*");
                    Matcher mInfluencias = regexInfluencias.matcher(xmlString);

                    log.info("****PASO 7: obtiene las relaciones: ");
                    if (mInfluencias.find()) {
                        Pattern regexOtrosPersonajes = Pattern.compile("(?:\\[\\[)(.)*?(?:\\]\\])");
                        Matcher mOP = regexOtrosPersonajes.matcher(mInfluencias.group(0));

                        while (mInfluencias.find() && mOP.find()) {
                            String nombreRelacionado = mOP.group(0);
                            nombreRelacionado = nombreRelacionado.replaceAll("[^A-Za-z ,]","").trim();
                            log.info("****PASO 7.1: obtiene las personas relacionadas: " + nombreRelacionado);
                            personajeEncontrado.addRelacionado(new Personaje(null, nombreRelacionado, null, null));
                        }
                    }
                    context.write(new Text(personajeEncontrado.toString()), new IntWritable(1));
                }
            }

        } catch (NumberFormatException | IOException | InterruptedException e) {
            throw new IOException(e);

        }
    }

}
