package ntw;

import java.util.ArrayList;

public interface ApplicationNtwServeur {

    ArrayList<Object> traiter_arrivee_reseau(ArrayList<Object> requete);

    void afficher_utilisateur(String texte);
}
