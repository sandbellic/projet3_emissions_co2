
import streamlit as st
from collections import deque, defaultdict

def reset_km():
    st.session_state["distance_input"] = 0
    st.session_state.calcul = False

def reset_itineraire():
    st.session_state["depart_selectbox"] = None
    st.session_state["arrivee_selectbox"] = None
    st.session_state["ar"] = False
    st.session_state.calcul = False   


def liste_villes_selection(df_villes):
    # ajout des dpératements dans les labels des villes pour les différencier en cas de doublons de noms de villes
    df_villes["label"] = df_villes["name"] + " (" + df_villes["dep_code"].astype(str) + ")"

    villes = df_villes["label"].tolist()

    #on va mettre quelques unes des villes les plus demandées en haut de la liste, 
    # et les autres villes seront par ordre alphabétique en dessous
    # Liste des villes prioritaires
    prioritaires = ["Paris (75)", "Lyon (69)", "Marseille (13)", "Toulouse (31)", "Nice (06)","Nantes (44)", "Bordeaux (33)", "Lille (59)"]

    # on enlève les villes prioritaires de la liste complète pour éviter de les avoior en double dans la liste finale,
    # et on classe la liste des autres villes par ordre alphabétique
    villes_prioritaires = [v for v in prioritaires if v in villes]
    autres_villes = sorted([v for v in villes if v not in prioritaires])

    # on combine les deux listes pour avoir les villes prioritaires en haut et les autres ensuite
    villes_finales = villes_prioritaires + autres_villes
    return villes_finales


def etapes_chemin(row, df_villes):
    chemin = row["chemin"]
    if not chemin:
        chemin_villes = ""
    elif "➞" in str(chemin):
        chemin_villes = chemin
    else:
        chemin_villes = []
        for c in chemin:
            correspondance = df_villes.loc[df_villes['id_commune'] == c,'name']
            if not correspondance.empty:
                chemin_villes.append(correspondance.iloc[0])
            else:
                chemin_villes.append(f"ID gare inconnu ({c})")
        chemin_villes = " ➞ ".join(chemin_villes)
    return chemin_villes






