package ntw;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimbreTemporel {

    SimpleDateFormat formatDate = new SimpleDateFormat("dd/MM/yy - HH:mm:ss");

    public String getTime() {
        Calendar now = Calendar.getInstance();
        return "\n[" + formatDate.format(now.getTime()) + "] ";
    }
}
