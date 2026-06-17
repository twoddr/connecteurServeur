package ntw;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnecteurServeur {

    private static final int CODE_BROADCAST = 40;
    private final int HASH_BROADCAST = -1111;
    private final ApplicationNtwServeur mainProgram;
    private final int NBRE_MAX_SOCKETS_MEMORISES;
    private final List<Connexion> clientsConnectes = new CopyOnWriteArrayList<>();
    // Pool de threads pour traiter les requêtes simultanément
    private final ScheduledExecutorService threadPool;
    private final AtomicInteger pendingRequests = new AtomicInteger(0);
    // Configuration et logging
    private final ServeurConfig config;
    private final ServeurLogger logger = ServeurLogger.INSTANCE;
    private final AtomicInteger nextConnectionId = new AtomicInteger(1);
    // variables d'état
    protected AtomicInteger nbrRecus = new AtomicInteger();
    private int port = 31416;
    private AtomicInteger nbrEnvoyes = new AtomicInteger();
    private ServerSocket serverSocket;
    private volatile WatchDogTimer watchDogTimer;
    private ScheduledFuture<?> heartbeatTask;

    // Serveur HTTP pour transfert de fichiers multipart
    private HttpFileServer httpFileServer;

    // constructeurs

    /**
     * Constructeur de base; le port vaudra la valeur par défaut
     * Une connexion est lancée automatiquement (ouverture du port)
     *
     * @param r             application appelante
     * @param maxConnexions le nombre max de clients autorisés
     */
    public ConnecteurServeur(ApplicationNtwServeur r, int maxConnexions) {
        mainProgram = r;
        this.config = new ServeurConfig();
        this.port = config.getServeurPort();

        // Configurer le logger
        logger.configure(config.getLogLevel(), config.getLogFormat(), mainProgram);

        // Créer un pool de threads avec la configuration
        this.threadPool = Executors.newScheduledThreadPool(Math.max(maxConnexions + 2, config.getThreadPoolCoreSize()));

        logger.info("ConnecteurServeur", "Démarrage avec configuration: " + config);
        connexionReseau();
        NBRE_MAX_SOCKETS_MEMORISES = maxConnexions;
    }

    /**
     * Constructeur de base;
     * Le port est ouvert (si disponible) et le serveur se met en attente
     *
     * @param portX         le port de connexion
     * @param maxConnexions ne nombre max de clients autorisés
     */
    public ConnecteurServeur(ApplicationNtwServeur r, Integer portX, int maxConnexions) {
        mainProgram = r;
        this.config = new ServeurConfig();
        this.port = portX != null ? portX : config.getServeurPort();

        // Configurer le logger
        logger.configure(config.getLogLevel(), config.getLogFormat(), mainProgram);

        // Créer un pool de threads avec la configuration
        this.threadPool = Executors.newScheduledThreadPool(Math.max(maxConnexions + 2, config.getThreadPoolCoreSize()));

        logger.info("ConnecteurServeur", "Démarrage avec port personnalisé " + port + " et configuration: " + config);
        NBRE_MAX_SOCKETS_MEMORISES = maxConnexions;
        connexionReseau();
    }

    // méthodes

    /**
     * établit une connexion vers le port contenu dans l'objet en cours Ne fait rien
     * si une
     * connexion est déjà établie
     */
    public final void connexionReseau() {
        if (serverSocket == null || serverSocket.isClosed()) {
            logger.info("ConnecteurServeur", "Tentative d'ouverture du port de communication sur " + port);
            try {
                serverSocket = new ServerSocket(port, 100);
                // serverSocket.setSoTimeout(config.getWatchdogTimeout()); // Timeout du serveur

                logger.info("ConnecteurServeur", "Connexion réussie sur " + serverSocket);
                demarrerHttpFileServer();
                lancer_boucle_reseau();
                // Heartbeat checker avec configuration
                if (heartbeatTask == null || heartbeatTask.isCancelled() || heartbeatTask.isDone()) {
                    heartbeatTask = threadPool.scheduleAtFixedRate(
                            new HeartbeatChecker(),
                            config.getHeartbeatInterval(),
                            config.getHeartbeatInterval(),
                            TimeUnit.MILLISECONDS);
                }
            } catch (IOException e) {
                String message = "Impossible d'ouvrir le port " + port + " ; il est probablement déjà pris !";
                logger.error("ConnecteurServeur", message);
            }
            return;
        }
        logger.warn("ConnecteurServeur", "Tentative de connexion alors que déjà connecté");
    }

    private void lancer_boucle_reseau() {
        Thread boucle_reseau = new Thread() {
            @Override
            public void run() {
                while (serverSocket != null && !serverSocket.isClosed()) {
                    try {
                        logger.info("ConnecteurServeur", "Le serveur est en contact avec " +
                                getNombreConnexions() + " client(s)");
                        LocalDateTime debut = LocalDateTime.now();

                        Socket socketClient = serverSocket.accept();

                        logger.info("ConnecteurServeur", "Serveur en attente d'un nouveau paquet (n° " +
                                (nbrRecus.incrementAndGet()) + ")...");

                        lancer_timer();
                        logger.info("ConnecteurServeur", "Nouvelle connexion en provenance de : " +
                                socketClient.getInetAddress() + "/" + socketClient.getPort());

                        // Configurer le socket client pour plus de robustesse
                        socketClient.setSoTimeout(config.getSocketTimeout());
                        socketClient.setKeepAlive(true);
                        socketClient.setTcpNoDelay(true);

                        // !! on ne peut pas initialiser l'inputStream tant que des données ne sont pas
                        // arrivées !!
                        // ObjectInputStream ois = new ObjectInputStream(socketClient.getInputStream());
                        ObjectOutputStream oos = new ObjectOutputStream(socketClient.getOutputStream());
                        oos.flush(); // Crucial pour débloquer le New ObjectInputStream du client

                        ObjectInputStream ois;
                        try {
                            ois = new ObjectInputStream(socketClient.getInputStream());
                        } catch (EOFException e) {
                            logger.warn("ConnecteurServeur", "Client fermé la connexion avant d'envoyer des données: " +
                                    socketClient.getInetAddress() + "/" + socketClient.getPort());
                            socketClient.close();
                            continue;
                        }
                        Connexion connexion = new Connexion(nextConnectionId.getAndIncrement(), socketClient, ois, oos);

                        Duration attente = Duration.between(debut, LocalDateTime.now());
                        logger.info("ConnecteurServeur", "Reçu connexion du client " +
                                connexion.getSocket() + " après " + attente.getSeconds() + " seconde(s) d'attente");
                        logger.debug("ConnecteurServeur", "Liste des connexions actives : " + clientsConnectes);

                        // lancer l'écoute permanente du client
                        if (sauver_connexion(connexion)) {
                            new TraiteurArriveeReseau(connexion).start();
                        } else {
                            logger.warn("ConnecteurServeur",
                                    "Connexion ignorée car trop de clients (max: " + NBRE_MAX_SOCKETS_MEMORISES + ")");
                            fermerConnexionSilencieuse(connexion);
                        }
                    } catch (SocketException e) {
                        if (serverSocket == null || serverSocket.isClosed()) {
                            logger.info("ConnecteurServeur", "Boucle réseau arrêtée : serverSocket fermé.");
                            break;
                        }
                        logger.error("ConnecteurServeur", "Erreur socket dans la boucle réseau: " + e.getMessage());
                    } catch (Exception e) {
                        logger.error("ConnecteurServeur", "Erreur dans la boucle réseau: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        arreter_timer();
                    }
                }
            }
        };
        boucle_reseau.setName("ThreadDeBoucleReseau");
        boucle_reseau.start();

    }

    private void fermerConnexionSilencieuse(Connexion connexion) {
        if (connexion == null)
            return;
        try {
            if (connexion.getObjectInputStream() != null)
                connexion.getObjectInputStream().close();
        } catch (Exception ignored) {
        }
        try {
            if (connexion.getObjectOutputStream() != null)
                connexion.getObjectOutputStream().close();
        } catch (Exception ignored) {
        }
        try {
            if (connexion.getSocket() != null && !connexion.getSocket().isClosed())
                connexion.getSocket().close();
        } catch (Exception ignored) {
        }
    }

    private void nettoyerConnexionsFermees() {
        clientsConnectes.removeIf(c -> {
            boolean closed = c == null
                    || c.getSocket() == null
                    || c.getSocket().isClosed();

            if (closed) {
                println("Client retiré (socket fermée) : " + c);
            }

            return closed;
        });
    }

    /**
     * Permet de voir si le serveur a des clients connectés
     *
     * @return vrai si le il existe une connexion en cours
     */
    public boolean isWorking() {
        nettoyerConnexionsFermees();
        return !clientsConnectes.isEmpty();
    }

    public Boolean isConnected() {
        if (serverSocket == null) {
            return false;
        }
        return !serverSocket.isClosed();
    }

    /**
     * Permet de déconnecter le réseau
     */
    public void deconnexionReseau() {
        if (serverSocket != null) {
            if (!serverSocket.isClosed()) {
                try {
                    if (threadPool != null && !threadPool.isShutdown()) {
                        if (heartbeatTask != null) {
                            heartbeatTask.cancel(true);
                            heartbeatTask = null;
                        }
                        threadPool.shutdown();
                        if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                            threadPool.shutdownNow();
                        }
                    }

                    // Arrêter le serveur HTTP
                    arreterHttpFileServer();

                    clientsConnectes.forEach(this::fermerConnexionSilencieuse);
                    clientsConnectes.clear();

                    serverSocket.close();
                    println("* Connexion fermée avec succès.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    println("*** Déconnexion interrompue pour le port " + port);
                } catch (IOException e) {
                    println("*** Exception attrapée : Impossible de fermer le port " + port);
                }
            } else {
                println("** Déjà déconnecté ! Déconnexion impossible !");
            }
        }
    }

    // Better packet validation avec limites de taille
    private boolean validatePacket(Object paquetReseau) {
        if (!(paquetReseau instanceof ArrayList<?> list)) {
            return false;
        }
        if (list.size() < 2) {
            return false;
        }
        if (!(list.get(0) instanceof Integer)) {
            return false;
        }
        return list.get(1) instanceof ArrayList<?>;
    }

    private void println(String txt) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        mainProgram.afficher_utilisateur(String.format("[%s] %s - %s", timestamp, " ConnecteurServeur -> ", txt));
    }

    // méthodes get/set
    public int getPort() {
        return port;
    }

    /**
     * Retourne le nombre de requêtes en attente de traitement
     */
    public int getPendingRequests() {
        return pendingRequests.get();
    }

    /**
     * Retourne des statistiques sur le pool de threads
     */
    public String getThreadPoolStats() {
        if (threadPool instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor tpe = (ThreadPoolExecutor) threadPool;
            return String.format("Pool[Active:%d, Queue:%d, Completed:%d]",
                    tpe.getActiveCount(), tpe.getQueue().size(), tpe.getCompletedTaskCount());
        }
        return "Pool stats non disponibles";
    }

    /**
     * Traitement du paquet reçu : va à la BD et revient
     * Utilise un pool de threads pour permettre le traitement simultané
     *
     * @param requete   paquet à ne pas toucher; c'est pour Levis
     * @param connexion connexion du client pour la réponse
     * @param hash      hash du paquet pour la réponse
     * @param numero    numéro du paquet pour le logging
     */
    private void envoi_vers_bd_async(ArrayList<Object> requete, Connexion connexion, int hash, int numero) {
        pendingRequests.incrementAndGet();

        threadPool.submit(() -> {
            try {
                println(">>> Début traitement paquet n°" + numero + " dans le pool de threads <<<");

                if (requete != null) {
                    println("* Requête envoyée à la BD : " + requete);
                    // --- Cette requête prend un temps indéterminé; on va le compter ---
                    LocalTime debut = LocalTime.now();
                    ArrayList<Object> reponseBD = mainProgram.traiter_arrivee_reseau(requete);
                    Duration duree = Duration.between(debut, LocalTime.now());
                    println("La requête vers la BD a duré : " + duree.toMillis() + " ms");

                    // Envoyer la réponse au client
                    println(">>> Fin traitement paquet n°" + numero + " - envoi réponse <<<");
                    renvoyer_reponse(reponseBD, connexion, hash, numero);
                } else {
                    println("***\nObjet reçu NULL ! \n***\n");
                    ArrayList<Object> packARenvoyer = new ArrayList<>();
                    packARenvoyer.add(-10);
                    ArrayList<Boolean> alb = new ArrayList<>();
                    alb.add(false);
                    packARenvoyer.add(alb);
                    renvoyer_reponse(packARenvoyer, connexion, hash, numero);
                }
            } catch (Exception e) {
                println("*** Erreur lors du traitement de la requête " + numero + " : " + e.getMessage());
                // Envoyer une réponse d'erreur au client
                ArrayList<Object> erreur = new ArrayList<>();
                erreur.add("Erreur serveur lors du traitement de la requête");
                renvoyer_reponse(erreur, connexion, hash, numero);
            } finally {
                pendingRequests.decrementAndGet();
                println(">>> Requête n°" + numero + " terminée. Requêtes en attente : " + pendingRequests.get()
                        + " <<<");
            }
        });
    }

    private void envoi_1_paquet_vers_reseau(ArrayList<Object> packAEnvoyer,
            Connexion connexion,
            int hash, int numero) {
        if (connexion == null || connexion.getSocket() == null || connexion.getSocket().isClosed()) {
            println("*** Connexion fermée ou null, impossible d'envoyer le paquet " + numero);
            return;
        }

        try {
            ArrayList<Object> packEncapsule = new ArrayList<>();
            packEncapsule.add(hash);
            packEncapsule.add(packAEnvoyer);
            synchronized (connexion.getObjectOutputStream()) { // ← protect the shared stream
                ObjectOutputStream oos = connexion.getObjectOutputStream();
                oos.writeObject(packEncapsule);
                oos.flush(); // force l'envoi immédiat
            }
            connexion.updateLastActivity();
            nbrEnvoyes.incrementAndGet();
            println("Réponse au paquet numéro " + numero + " renvoyée vers le client ! ");
            println("Nombre total d'envois : " + nbrEnvoyes);
        } catch (Exception e) {
            println("*** ConnecteurServeur : Problème IO lors de l'envoi vers le réseau => " +
                    packAEnvoyer);
            println("** Erreur: " + e.getClass().getSimpleName() + " - " + e.getMessage());
            // Marquer la connexion comme fermée
            fermerConnexionSilencieuse(connexion);
            clientsConnectes.remove(connexion);
        }
    }

    private void broadcast_reseau(ArrayList<Object> packAEnvoyer,
            Connexion originaire,
            int hashOriginaire) {
        println("***INFO*ConnecteurServeur**\nBroadcast initié vers les sockets : " + clientsConnectes
                + "\n***");
        // envoi vers l'originaire de la requête en priorité (car on connait son hash)
        envoi_1_paquet_vers_reseau(packAEnvoyer, originaire, hashOriginaire, nbrEnvoyes.incrementAndGet());
        println("Broadcast envoyé mais originaire servi en premier : " + originaire);
        ArrayList<Connexion> bannis = new ArrayList<>();
        for (Connexion client : clientsConnectes) {
            Socket socketClient = client.getSocket();
            println("* Le connecteurServeur a réussi à créer le socketClient pour la réponse ;-)");
            if (socketClient.isClosed()) {
                bannis.add(client);
                println("* ConnecteurServeur/broadcast_reseau : Le client " + socketClient + " n'est plus connecté !" +
                        "\nIl a été retiré de la liste !");
            } else {
                if (!client.equals(originaire)) { // originaire déjà servi
                    envoi_1_paquet_vers_reseau(packAEnvoyer, client, 0, nbrEnvoyes.incrementAndGet());
                }
            }
        }
        for (Connexion clientBanni : bannis) {
            fermerConnexionSilencieuse(clientBanni);
            clientsConnectes.remove(clientBanni);
        }
    }

    private void arreter_timer() {
        try {
            if (watchDogTimer != null) {
                watchDogTimer.interrupt();
            }
        } catch (Exception e) {
            println("*** Erreur arrêt watchdog : " + e.getMessage());
        } finally {
            watchDogTimer = null;
        }
    }

    private void lancer_timer() {
        watchDogTimer = new WatchDogTimer();
        try {
            watchDogTimer.start();
        } catch (Exception e) {
            println("*** Watchdog du connecteurServeur : je ne peux démarrer un process en cours !");
        }
    }

    private void renvoyer_reponse(ArrayList<Object> retour,
            Connexion connexion,
            int hash, int numero) {
        Integer codeRetour = null;
        if (retour.size() > 3 && retour.get(3) instanceof Integer) {
            codeRetour = (int) retour.get(3);
        }
        if (codeRetour != null && codeRetour == CODE_BROADCAST) {
            println("* BROADCAST détecté !");
            broadcast_reseau(retour, connexion, hash);
            return;
        }

        envoi_1_paquet_vers_reseau(retour, connexion, hash, numero);
    }

    /**
     * Donne une connexion qui se trouve dans la liste des stockées
     * De plus, on ferme la plus ancienne connexion si le nombre max est atteint
     *
     * @param connexionTest la connexion à rechercher dans la liste des stockées
     */
    private boolean sauver_connexion(Connexion connexionTest) {
        nettoyerConnexionsFermees();

        if (getNombreConnexions() >= NBRE_MAX_SOCKETS_MEMORISES) {
            println("* ConnecteurServeur : nombre max de connexions atteint : "
                    + NBRE_MAX_SOCKETS_MEMORISES);
            return false;
        }

        clientsConnectes.add(connexionTest);
        println(">>> Connexion sauvegardée : " + connexionTest);
        println(">>> Nombre de clients en mémoire : " + getNombreConnexions());
        return true;
    }

    private int getNombreConnexions() {
        nettoyerConnexionsFermees();
        return clientsConnectes.size();
    }

    private String getTimeStamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy - HH:mm"));
    }

    // ============================================================
    // SERVEUR HTTP POUR TRANSFERT DE FICHIERS MULTIPART
    // ============================================================

    /**
     * Démarre le serveur HTTP pour le transfert de fichiers multipart.
     * Écoute sur le port configuré dans serveur.properties (serveur.http.port,
     * défaut: 8080).
     */
    private void demarrerHttpFileServer() {
        int httpPort = config.getHttpPort();
        if (httpPort <= 0) {
            logger.info("ConnecteurServeur", "Serveur HTTP désactivé (port=" + httpPort + ")");
            return;
        }
        httpFileServer = new HttpFileServer(httpPort, mainProgram, logger);
        httpFileServer.start();
        logger.info("ConnecteurServeur", "Serveur HTTP démarré sur le port " + httpPort);
    }

    /**
     * Arrête le serveur HTTP.
     */
    private void arreterHttpFileServer() {
        if (httpFileServer != null) {
            httpFileServer.stop();
            httpFileServer = null;
        }
    }

    /*
     **************** classes internes
     */
    private class TraiteurArriveeReseau extends Thread {
        Connexion connexion;

        public TraiteurArriveeReseau(Connexion connexionClient) {
            connexion = connexionClient;
            this.setName("ClientHandler-" + connexionClient.getSocket().getPort());
        }

        @Override
        public void run() {
            try {
                while (!connexion.getSocket().isClosed()) {
                    try {
                        // attendre le paquet suivant
                        Object paquetReseau = connexion.getObjectInputStream().readObject();

                        // Paquet reçu avec succès : incrémenter le compteur maintenant
                        int numero = nbrRecus.incrementAndGet();

                        // Mettre à jour l'activité dès réception
                        connexion.updateLastActivity();

                        println(">> Reçu le paquet " + numero + " à : " + getTimeStamp());
                        if (!validatePacket(paquetReseau)) {
                            println("*** ConnecteurServeur : Mauvais format reçu. J'attends (int, Arraylist) " +
                                    "mais je reçois => " + paquetReseau + "\nJe jette !");
                            continue; // on reste dans la boucle !
                        }

                        int hash = (int) ((ArrayList) paquetReseau).get(0);
                        ArrayList<Object> requete = (ArrayList<Object>) ((ArrayList) paquetReseau).get(1);
                        println("* Le paquet " + numero + " envoyé à Levis (BD) => " + requete);
                        println("* Nombre de clients connectés : " + getNombreConnexions());

                        // Soumettre le traitement au pool de threads (plus de synchronisation globale
                        // !)
                        envoi_vers_bd_async(requete, connexion, hash, numero);
                    } catch (SocketTimeoutException e) {
                        // Timeout normal : ne pas incrémenter le compteur, juste continuer l'écoute
                        // On ne log rien pour éviter de polluer les logs
                        continue;
                    } catch (EOFException | SocketException e) {
                        // Déconnexion client : sortir proprement
                        println("Client déconnecté (" + e.getClass().getSimpleName() + ") : " + e.getMessage());
                        println("\n ===> Nombre de clients connectés actuellement : " + getNombreConnexions() + "\n");
                        return; // → finally nettoie
                    } catch (ClassNotFoundException e) {
                        // Paquet illisible : on log et on continue la boucle
                        println("*** ClassNotFoundException, paquet ignoré : " + e.getMessage());
                    } catch (Exception e) {
                        // Autre erreur sur CE paquet : on log, on continue
                        println("*** Exception sur réception paquet " + (nbrRecus.get() + 1) + " : "
                                + e.getClass().getSimpleName() + " - " + e.getMessage());
                    }
                }
                println("TraiteurArriveeReseau : Le socket " + connexion.getSocket() + " est fermé ! ");
                println("\n ===> Nombre de clients connectés actuellement : " + getNombreConnexions() + "\n");
            } finally {
                fermerConnexionSilencieuse(connexion);
                clientsConnectes.remove(connexion);
                println("Connexion retirée : " + connexion);
            }
        }

    }

    /*
     ********* HEARTBEAT CHECKER **************
     */
    private class HeartbeatChecker implements Runnable {
        @Override
        public void run() {
            try {
                // Nettoyer les connexions fermées (détection des clients déconnectés)
                nettoyerConnexionsFermees();
            } catch (Exception e) {
                logger.error("HeartbeatChecker", "Erreur lors du nettoyage: " + e.getMessage());
            }
        }
    }

    /*
     *************** WATCHDOG ***********************
     */
    class WatchDogTimer extends Thread {
        @Override
        public void run() {
            try {
                // Watchdog de 2 minutes: si aucun paquet n'arrive pendant 2 minutes, on ferme
                sleep(120000L);
                println("***  Watchdog expiré après 2 minutes d'inactivité sur cette connexion !");
                // Ne pas arrêter le serveur entier, juste interrompre ce thread
            } catch (InterruptedException e) {
                // C'est normal, le watchdog a été interrompu (activité reçue)
            }
        }

    }

    /*
     ******* CONNEXIONS ************
     */
    private class Connexion {
        private final int connectionId;
        private Socket socket;
        private ObjectOutputStream objectOutputStream;
        private ObjectInputStream objectInputStream;
        private LocalDateTime localDateTime;
        private volatile LocalDateTime lastActivityTime;

        public Connexion(int idX, Socket sock, ObjectInputStream ois, ObjectOutputStream oos) {
            socket = sock;
            connectionId = idX;
            objectOutputStream = oos;
            objectInputStream = ois;
            localDateTime = LocalDateTime.now();
            this.lastActivityTime = LocalDateTime.now();
        }

        public Socket getSocket() {
            return socket;
        }

        public ObjectOutputStream getObjectOutputStream() {
            return objectOutputStream;
        }

        public LocalDateTime getLocalDateTime() {
            return localDateTime;
        }

        public void updateLastActivity() {
            this.lastActivityTime = LocalDateTime.now();
        }

        public LocalDateTime getLastActivityTime() {
            return lastActivityTime;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof Connexion other))
                return false;
            return this.connectionId == other.connectionId;
        }

        public int getConnectionId() {
            return connectionId;
        }

        @Override
        public String toString() {
            return "Connexion{id=" + connectionId +
                    ", remote=" + socket.getRemoteSocketAddress() + "}";
        }

        public ObjectInputStream getObjectInputStream() {
            return objectInputStream;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(connectionId);
        }
    }

}
