package ntw;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;

public class ConnecteurServeur {

    private static final int CODE_BROADCAST = 40;
    private static TraiteurArriveeReseau ecouteurReseau;
    private final int HASH_BROADCAST = -1111;
    private final ApplicationNtwServeur mainProgram;
    private final int NBRE_MAX_SOCKETS_MEMORISES;
    // variables d'état
    protected int nbrRecus = 0;
    private TreeSet<Connexion> clientsConnectes = new TreeSet<>();
    private int port = 31416;
    private int nbrEnvoyes = 0;
    private ServerSocket serverSocket;
    private WatchDogTimer watchDogTimer;

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
        port = portX;
        NBRE_MAX_SOCKETS_MEMORISES = maxConnexions;
        connexionReseau();
    }


    // méthodes

    /**
     * établit une connexion vers le port contenu dans l'objet en cours Ne fait rien si une
     * connexion est déjà établie
     */
    public final void connexionReseau() {
        if (serverSocket == null || serverSocket.isClosed()) {
            println("* Tentative d'ouverture du port de communication sur ...");
            try {
                serverSocket = new ServerSocket(port, 100);
                println("..." + serverSocket + " réussie ! ;-)");
                lancer_boucle_reseau();
            } catch (IOException e) {
                println("*** ..." + serverSocket + " Échec ! Impossible d'ouvrir le port " + port + " ; il est probablement déjà pris !");
            }
            return;
        }
        println("** Tentative de connexion alors que déjà  connecté :-|");
    }

    private void lancer_boucle_reseau() {
        Thread boucle_reseau = new Thread() {
            @Override
            public void run() {
                while (!serverSocket.isClosed()) {
                    try {
                        println(">> Le serveur est en contact avec " + clientsConnectes.size() + " client (s)");
                        println(">> Serveur en attente d'un nouveau paquet (n° " + (nbrRecus + 1) + ") ..." +
                                "\n*************************************\n\n");
                        LocalDateTime debut = LocalDateTime.now();
                        Socket socketClient = serverSocket.accept();

                        lancer_timer();
                        println("\n<<<<<<<\n" +
                                "Nouvelle connexion en provenance de : " + socketClient.getInetAddress() + "/" +
                                socketClient.getPort());

                        // !! on ne peut pas initialiser l'inputStream tant que des données ne sont pas arrivées !!
                        //ObjectInputStream ois = new ObjectInputStream(socketClient.getInputStream());
                        ObjectOutputStream oos = new ObjectOutputStream(socketClient.getOutputStream());
                        Connexion connexion = new Connexion(socketClient, oos);

                        Duration attente = Duration.between(debut, LocalDateTime.now());
                        println("Reçu connexion du client : "
                                + connexion.getSocket() + " après " + attente.getSeconds() + " seconde(s) d'attente");
                        println("Liste des connexions actives : " + clientsConnectes);

                        // lancer l'écoute permanente du client
                        if (sauver_connexion(connexion)) {// sauver le client
                            ecouteurReseau = new TraiteurArriveeReseau(connexion);
                            ecouteurReseau.start();
                        } else {
                            println("\n !!! Connexion ignorée par le serveur car trop de clients !!! \n");
                        }

                        arreter_timer();
                    } catch (Exception e) {
                        println("*** Erreur entrée-sortie du socket réseau : " + e);
                        e.printStackTrace();
                    }
                }
                println("*** Fermeture inopinée de la connexion ; je tente de la relancer ...");
                connexionReseau();
            }
        };
        boucle_reseau.start();

    }

    /**
     * Permet de voir si le serveur tourne
     *
     * @return vrai si le thread de l'écoute serveur est vivant
     */
    public boolean isWorking() {
        return ecouteurReseau != null && ecouteurReseau.isAlive();
    }

    public Boolean isConnected() {
        return !serverSocket.isClosed();
    }

    /**
     * Permet de déconnecter le réseau
     */
    public void deconnexionReseau() {
        if (serverSocket != null) {
            if (!serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                    println("* Connexion fermée avec succès.");
                } catch (IOException e) {
                    println("*** Exception attrapée : Impossible de fermer le port " + port);
                }
            } else {
                println("** Déjà déconnecté ! Déconnexion impossible !");
            }
        }
    }

    private void println(String txt) {
        mainProgram.afficher_utilisateur("ConnecteurServeur -> " + txt);
    }


    // méthodes get/set
    public int getPort() {
        return port;
    }

    /**
     * Traitement du paquet reçu : va à la BD et revient
     * La méthode est synchronizée; tous les threads font la file et attendent leur tour
     *
     * @param requete paquet à ne pas toucher; c'est pour Levis
     * @return le paquet à ne pas toucher, fourni par Levis
     */
    private synchronized ArrayList<Object> envoi_vers_bd(ArrayList<Object> requete, int numero) {
        println(">>> Prise de la clé de synchronisation par le paquet n°" + numero + " <<<");
        if (requete != null) {
            println("* Requête envoyée à la BD : " + requete);
            // --- Cette requête prend un temps indéterminé; on va le compter ---
            LocalTime debut = LocalTime.now();
            ArrayList<Object> reponseBD = mainProgram.traiter_arrivee_reseau(requete);
            Duration duree = Duration.between(debut, LocalTime.now());
            println("La requête vers la BD a duré : " + duree.toMillis() + " ms");
            println(">>> Rendu de la clé de synchronisation par le paquet n°" + numero + " <<<");
            return reponseBD;
        }
        println("***\nObjet reçu NULL ! \n***\n");
        ArrayList<Object> packARenvoyer = new ArrayList<>();
        packARenvoyer.add(-10);
        ArrayList<Boolean> alb = new ArrayList<>();
        alb.add(false);
        packARenvoyer.add(alb);
        println(">>> Rendu de la clé de synchronisation <<<");
        return packARenvoyer;
    }


    private void envoi_1_paquet_vers_reseau(ArrayList<Object> packAEnvoyer,
                                            Connexion connexion,
                                            int hash, int numero) {
        try {
            ArrayList<Object> packEncapsule = new ArrayList<>();
            packEncapsule.add(hash);
            packEncapsule.add(packAEnvoyer);
            ObjectOutputStream oos = connexion.getObjectOutputStream();
            oos.writeObject(packEncapsule);
            oos.flush(); // force l'envoi immédiat
            //oos.reset();  // Clear internal caches to avoid type code issues

            nbrEnvoyes++;
            println("Réponse au paquet numéro " + numero + " renvoyée vers le client ! ");
            println("Nombre total d'envois : " + nbrEnvoyes);
        } catch (Exception e) {
            println("*** ConnecteurServeur : Problème IO lors de l'envoi vers le réseau => " +
                    packAEnvoyer);
            println("** voici le stacktrace : " + e.getMessage());
        }
    }

    private void broadcast_reseau(ArrayList<Object> packAEnvoyer) {
        println("***INFO*ConnecteurServeur**\nBroadcast initié vers les sockets : " + clientsConnectes
                + "\n***");
        ArrayList<Connexion> bannis = new ArrayList<>();
        for (Connexion client : clientsConnectes) {
            Socket socketClient = client.getSocket();
            println("* Le connecteurServeur a réussi à créer le socketClient pour la réponse ;-)");
            if (socketClient.isConnected()) {
                envoi_1_paquet_vers_reseau(packAEnvoyer, client, 0, nbrEnvoyes++);
            } else {
                bannis.add(client);
                println("* ConnecteurServeur/broadcast_reseau : Le client " + socketClient + " n'est plus connecté !" +
                        "\nIl a été retiré de la liste !");
            }
        }
        bannis.forEach(clientsConnectes::remove);
    }


    private void arreter_timer() {
        try {
            if (watchDogTimer != null) {
                watchDogTimer.interrupt();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            broadcast_reseau(retour);
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
        Collection<Connexion> deadLinks = new ArrayList<>();
        boolean trouve = false;
        for (Connexion connexion : clientsConnectes) {
            if (connexion.getSocket().isClosed()) {
                deadLinks.add(connexion);
            } else if (connexion.equals(connexionTest)) {
                if (trouve) {
                    println("** Problème dans ConnecteurServeur/sauver_connexion : il existe deux sauvegardes " +
                            "pour le client " + connexion);
                } else {
                    trouve = true;
                    println("La connexion au Socket client " + connexion.getSocket() + " est déjà dans la liste stockée");
                }
            }
        }
        // nettoyer les cadavres
        deadLinks.forEach(clientsConnectes::remove);
// à présent, vérifier qu'on en a pas plus que nécessaire
        if (clientsConnectes.size() > NBRE_MAX_SOCKETS_MEMORISES) {
//            Connexion ancien = clientsConnectes.first();
//            clientsConnectes.remove(ancien);
            println("* ConnecteurServeur : Nombre max de connexions atteinte !");
            return false;
        }
        //  ajouter une nouvelle connexion
        clientsConnectes.add(connexionTest);
        println("\n>>> Socket client " + connexionTest.getSocket() + " sauvegardé");
        println("\n>>> Nombre de clients en mémoire : " + clientsConnectes.size());
        return true;
    }

    private int getNombreConnexions() {
        int nombre = 0;
        ArrayList<Connexion> bannis = new ArrayList<>();
        for (Connexion connexion : clientsConnectes) {
            if (connexion.getSocket().isClosed()) {
                bannis.add(connexion);
            } else {
                nombre++;
            }
        }
        bannis.forEach(clientsConnectes::remove);
        return nombre;
    }

    /*
     **************** classes internes
     */
    private class TraiteurArriveeReseau extends Thread {
        Connexion connexion;

        public TraiteurArriveeReseau(Connexion connexionClient) {
            connexion = connexionClient;
        }

        @Override
        public void run() {
            try {
                // créer l'inputstream seulement maintenant !
                ObjectInputStream ois = new ObjectInputStream(connexion.getSocket().getInputStream());
                while (!connexion.getSocket().isClosed()) {
                    int numero = nbrRecus++;
                    println("\n\nNombre de connexions actives = " + clientsConnectes.size());
                    println("* Serveur en écoute pour le paquet n° " + numero + "...");
                    // attendre le paquet suivant
                    Object paquetReseau = ois.readObject();

                    println(">> Reçu le paquet " + numero + " à : " +
                            LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy - HH:mm")));

                    if (paquetReseau instanceof ArrayList &&
                            !((ArrayList) paquetReseau).isEmpty() &&
                            ((ArrayList) paquetReseau).get(0) instanceof Integer &&
                            ((ArrayList) paquetReseau).get(1) instanceof ArrayList &&
                            !((ArrayList<?>) ((ArrayList<?>) paquetReseau).get(1)).isEmpty()) {

                        int hash = (int) ((ArrayList) paquetReseau).get(0);
                        ArrayList<Object> requete = (ArrayList<Object>) ((ArrayList) paquetReseau).get(1);
                        println("* Le paquet " + numero + " envoyé à Levis (BD) => " + requete);

                        // attendre le retour de la BD
                        // Attention : méthode synchronisée ! Il faut s'assurer qu'elle se termine proprement
                        ArrayList<Object> retour = envoi_vers_bd(requete, numero);
                        println("\n>>>>>>\n" + "* Réponse de la BD reçue pour le paquet n° " + numero + " => " + retour +
                                "\nElle va être retournée à : " + connexion);

                        // renvoyer la réponse vers l'outputstream
                        renvoyer_reponse(retour, connexion, hash, numero);
                    } else {
                        println("*** ConnecteurServeur : Mauvais format reçu. J'attends (int, Arraylist) " +
                                "mais je reçois => " + paquetReseau + "\nJe jette !");
                    }
                }
                println("TraiteurArriveeReseau : Le socket " + connexion.getSocket() + " est fermé ! Je " +
                        "supprime la connexion");
                clientsConnectes.remove(connexion);
                println("\n ===> Nombre de clients connectés actuellement : " + clientsConnectes.size() + "\n");
            } catch (EOFException e) {
                println("*** Problème IO à l'ouverture du flux d'entrée (connecteurServeur) \n" +
                        "C'est probablement dû à une fermeture de flux côté client !");
                println("* Retrait du client " + connexion + " des listes !");
                clientsConnectes.remove(connexion);
                println("\n ===> Nombre de clients connectés actuellement : " + clientsConnectes.size() + "\n");
            } catch (Exception e) {
                println("*** Problème IO à l'ouverture du flux d'entrée (connecteurServeur) \n" +
                        "Le paquet n'a pas été envoyé à la BD ; il a été ignoré !");
                // renvoyer la réponse vers l'outputstream
                ArrayList<Object> erreur = new ArrayList<>();
                erreur.add("Problème à la réception côté serveur : " + connexion);
                renvoyer_reponse(erreur, connexion, -1, nbrRecus);
                //e.printStackTrace();
                println("\n ===> Nombre de clients connectés actuellement : " + clientsConnectes.size() + "\n");
                /*println("* Retrait du client " + connexion + " des listes !");
                clientsConnectes.remove(connexion);*/
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
                sleep(30000L);
                println("***  Erreur : Watchdog  arrivé à expiration - Je relance le connecteurServeur !");
                deconnexionReseau();
                sleep(2000);
                connexionReseau();
            } catch (InterruptedException e) {
                //println("Info : Interruption du Wachdog");
            }

        }

    }

    /*
     ******* CONNEXIONS ************
     */
    private class Connexion implements Comparable {
        private Socket socket;
        private ObjectOutputStream objectOutputStream;
        private LocalDateTime localDateTime;

        public Connexion(Socket sock, ObjectOutputStream oos) {
            socket = sock;
            objectOutputStream = oos;
            localDateTime = LocalDateTime.now();
        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof Connexion) {
                Connexion autreConnexion = (Connexion) o;
                return autreConnexion.getSocket().getInetAddress().toString().compareTo(
                        getSocket().getInetAddress().toString());
            }
            return 1;
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


        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Connexion) {
                return socket.getInetAddress().toString()
                        .equals(((Connexion) obj).getSocket().getInetAddress().toString());
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            return getSocket().toString();
        }
    }

}
