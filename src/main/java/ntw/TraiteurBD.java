package ntw;

import java.util.ArrayList;

public interface TraiteurBD {
    /**
     * Transforme une requete locale en informations compréhensibles par
     * la BD distante
     *
     * @param objet ce sont les objets de la requete : recherche, critères, etc
     * @param code  c'est le code de transfert
     * @return une arraylist que la BD saura interpréter
     */
    public ArrayList convertirObjetVersRequete(int code, Object objet);

    /**
     * Convertit la réponse de la BD distante en objets locaux
     *
     * @param requete c'est la liste de réponses envoyées par la BD distante
     * @return une liste de données compréhensibles localement
     */
    public ArrayList convertirRequeteVersObjets(ArrayList requete);
}
