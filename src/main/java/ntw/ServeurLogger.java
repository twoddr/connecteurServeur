package ntw;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Logger structuré pour le ConnecteurServeur
 * Remplace les println() par un système de logging configurable
 */
public enum ServeurLogger {
    INSTANCE;
    
    public enum Level {
        DEBUG(0), INFO(1), WARN(2), ERROR(3);
        
        private final int value;
        Level(int value) { this.value = value; }
        public int getValue() { return value; }
    }
    
    private Level currentLevel = Level.INFO;
    private String logFormat = "[%s] %s - %s";
    private ApplicationNtwServeur appLogger;
    
    public void configure(Level level, String format, ApplicationNtwServeur logger) {
        this.currentLevel = level;
        this.logFormat = format;
        this.appLogger = logger;
    }
    
    public void debug(String component, String message) {
        log(Level.DEBUG, component, message);
    }
    
    public void info(String component, String message) {
        log(Level.INFO, component, message);
    }
    
    public void warn(String component, String message) {
        log(Level.WARN, component, message);
    }
    
    public void error(String component, String message) {
        log(Level.ERROR, component, message);
    }
    
    public void error(String component, String message, Throwable throwable) {
        log(Level.ERROR, component, message + " - " + throwable.getMessage());
    }
    
    private void log(Level level, String component, String message) {
        if (level.getValue() >= currentLevel.getValue() && appLogger != null) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            String formattedMessage = String.format(logFormat, timestamp, component, message);
            appLogger.afficher_utilisateur(formattedMessage);
        }
    }
}
