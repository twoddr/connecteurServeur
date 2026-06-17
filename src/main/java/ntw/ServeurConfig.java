package ntw;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Gestionnaire de configuration pour le ConnecteurServeur
 * Charge les paramètres depuis le fichier serveur.properties
 */
public class ServeurConfig {
    private static final String CONFIG_FILE = "serveur.properties";
    private final Properties properties;
    
    public ServeurConfig() {
        this.properties = new Properties();
        loadConfiguration();
    }
    
    private void loadConfiguration() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input != null) {
                properties.load(input);
            } else {
                // Valeurs par défaut si le fichier n'existe pas
                setDefaultValues();
            }
        } catch (IOException ex) {
            System.err.println("Erreur lors du chargement de la configuration: " + ex.getMessage());
            setDefaultValues();
        }
    }
    
    private void setDefaultValues() {
        properties.setProperty("serveur.port", "31416");
        properties.setProperty("serveur.threadPool.coreSize", "4");
        properties.setProperty("serveur.threadPool.maxSize", "50");
        properties.setProperty("serveur.timeout.socket", "30000");
        properties.setProperty("serveur.timeout.watchdog", "30000");
        properties.setProperty("serveur.heartbeat.interval", "60000");
        properties.setProperty("serveur.validation.maxPacketSize", "10");
        properties.setProperty("serveur.validation.maxRequestSize", "100");
        properties.setProperty("logging.level", "INFO");
        properties.setProperty("logging.format", "[%s] %s - %s");
    }
    
    public int getServeurPort() {
        return Integer.parseInt(properties.getProperty("serveur.port", "31416"));
    }
    
    public int getThreadPoolCoreSize() {
        return Integer.parseInt(properties.getProperty("serveur.threadPool.coreSize", "4"));
    }
    
    public int getThreadPoolMaxSize() {
        return Integer.parseInt(properties.getProperty("serveur.threadPool.maxSize", "50"));
    }
    
    public int getSocketTimeout() {
        return Integer.parseInt(properties.getProperty("serveur.timeout.socket", "30000"));
    }
    
    public int getWatchdogTimeout() {
        return Integer.parseInt(properties.getProperty("serveur.timeout.watchdog", "30000"));
    }
    
    public int getHeartbeatInterval() {
        return Integer.parseInt(properties.getProperty("serveur.heartbeat.interval", "60000"));
    }
    
    public int getMaxPacketSize() {
        return Integer.parseInt(properties.getProperty("serveur.validation.maxPacketSize", "10"));
    }
    
    public int getMaxRequestSize() {
        return Integer.parseInt(properties.getProperty("serveur.validation.maxRequestSize", "100"));
    }
    
    public ServeurLogger.Level getLogLevel() {
        String levelStr = properties.getProperty("logging.level", "INFO");
        try {
            return ServeurLogger.Level.valueOf(levelStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ServeurLogger.Level.INFO;
        }
    }
    
    public String getLogFormat() {
        return properties.getProperty("logging.format", "[%s] %s - %s");
    }
    
    @Override
    public String toString() {
        return "ServeurConfig{" +
                "port=" + getServeurPort() +
                ", threadPoolCore=" + getThreadPoolCoreSize() +
                ", socketTimeout=" + getSocketTimeout() +
                '}';
    }
}
