package ntw;

import java.util.ArrayList;

public class AfficheurRequete {

    public static String afficher_requete(ArrayList<Object> requete) {
        if (requete.isEmpty()) {
            return "(liste vide!)";
        }
        String contenu = "*";
        if (requete.get(0) instanceof String &&
                requete.size() > 1) { // nom de table
            contenu = String.valueOf(requete.get(0));
            return contenu + ", " + afficher_requete(new ArrayList<>(requete.subList(1, requete.size())));
        }
        if (!(requete.get(0) instanceof Integer)) {
            return String.valueOf(requete);
        }
        int code = (int) requete.get(0);
        String operation = convertir_code(code);
        if (requete.size() > 1) {
            Object elt1 = requete.get(1);
            if (elt1 instanceof ArrayList) {
                contenu = afficher_liste((ArrayList<Object>) elt1);
            } else if (elt1 instanceof String) {
                contenu = String.valueOf(elt1);
            } else {
                contenu = "Élément inattendu : " + elt1;
            }
        }
        String arguments = requete.size() > 2 ? "avec arguments" : "sans arguments";
        return operation + " - " + contenu + " - " + arguments;
    }

    private static String convertir_code(int code) {
        if (code < 0) {
            return "Échec (" + code + ")";
        }
        switch (code) {
            case 0:
                return "Connexion";
            case 1:
                return "Ajout";
            case 2:
                return "Modification";
            case 3:
                return "Suppression";
            case 4:
                return "Consultation";
            case 5:
                return "Demande numéro";
            case 6:
                return "Recherche spéciale";
            case 8:
                return "Login";
            case 9:
                return "Sauvegarde BD";
            default:
                return "Code " + code;
        }
    }

    private static String afficher_liste(ArrayList<Object> liste) {
        return "(Liste de taille " + liste.size() +
                (liste.isEmpty() ?
                        "" :
                        ", contenant " + (liste.get(0) instanceof String ?
                                liste.get(0) :
                                "des " + liste.get(0).getClass().getSimpleName()))
                + ")";
    }
}
