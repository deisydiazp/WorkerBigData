package uniandes.mapRed;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, Text, Text, Text> {

    public static final Log log = LogFactory.getLog(WCMapperFiltro.class);
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text textValue : values) {
            log.info("*******Llave WCReducer: " + key + " | Valor: " + textValue.toString());
            context.write(key, textValue);
        }

    }

}
