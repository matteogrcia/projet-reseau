#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <cjson/cJSON.h>

/* --- PARAMÈTRES RÉSEAU --- */
#define PORT_CONTROLE 5000
#define PORT_DONNEES 6000
#define TAILLE_TAMPON 2048
#define NOMBRE_MAX_NOEUDS 20

/* --- STRUCTURES DE DONNÉES --- */
typedef struct {
    char adresse_ip[16];
    int cout_direct;
} VoisinPhysique;

/* --- ÉTAT DE LA PASSERELLE --- */
char mon_ip[16];
char ip_racine_pmr[16];
int je_suis_la_racine = 0;

// Mesure de performance (Protégé par mutex car lu/écrit par les deux threads)
int mon_cout_actuel_vers_racine = 999;
char ip_de_mon_parent_actuel[16];

// Connaissance du réseau (issue du JSON)
VoisinPhysique topologie_reseau[NOMBRE_MAX_NOEUDS];
int nombre_voisins_physiques = 0;

// Membres actifs de l'arbre multicast (ceux à qui je dois envoyer les données)
char liste_voisins_multicast[NOMBRE_MAX_NOEUDS][16];
int nombre_voisins_multicast = 0;
// Note : Pour le JACK, on stocke aussi les coûts connus par la Racine
int couts_voisins_multicast[NOMBRE_MAX_NOEUDS];

// Terminaux (Microcores) rattachés à moi
char liste_terminaux_locaux[NOMBRE_MAX_NOEUDS][16];
int nombre_terminaux_locaux = 0;

pthread_mutex_t verrou_partage = PTHREAD_MUTEX_INITIALIZER;

/******************************************************************************
 * BLOC 1 : CHARGEMENT DE LA CARTE RÉSEAU (JSON)
 *****************************************************************************/
void charger_configuration(const char *nom_fichier) {
    FILE *fichier = fopen(nom_fichier, "rb");
    if (!fichier) { perror("Erreur ouverture JSON"); exit(1); }

    fseek(fichier, 0, SEEK_END);
    long longueur = ftell(fichier);
    fseek(fichier, 0, SEEK_SET);
    char *donnees_json = malloc(longueur + 1);
    fread(donnees_json, 1, longueur, fichier);
    fclose(fichier);

    cJSON *objet_json = cJSON_Parse(donnees_json);
    strcpy(ip_racine_pmr, cJSON_GetObjectItem(objet_json, "root_gateway")->valuestring);

    if (strcmp(mon_ip, ip_racine_pmr) == 0) {
        je_suis_la_racine = 1;
        mon_cout_actuel_vers_racine = 0;
    }

    cJSON *passerelles = cJSON_GetObjectItem(objet_json, "gateways");
    int taille_tableau = cJSON_GetArraySize(passerelles);
    for (int i = 0; i < taille_tableau; i++) {
        cJSON *item = cJSON_GetArrayItem(passerelles, i);
        const char* ip_trouvee = cJSON_GetObjectItem(item, "ip")->valuestring;
        if (strcmp(ip_trouvee, mon_ip) != 0) {
            strcpy(topologie_reseau[nombre_voisins_physiques].adresse_ip, ip_trouvee);
            topologie_reseau[nombre_voisins_physiques].cout_direct = cJSON_GetObjectItem(item, "cost")->valueint;
            nombre_voisins_physiques++;
        }
    }

    strcpy(ip_de_mon_parent_actuel, ip_racine_pmr);
    free(donnees_json);
    cJSON_Delete(objet_json);
}

/******************************************************************************
 * BLOC 2 : PLAN DE CONTRÔLE (SIGNALISATION ET OPTIMISATION)
 *****************************************************************************/
void* thread_controle(void* arg) {
    int descripteur_socket = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in adresse_ecoute = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE), .sin_addr.s_addr = INADDR_ANY };
    bind(descripteur_socket, (const struct sockaddr *)&adresse_ecoute, sizeof(adresse_ecoute));

    char message_recu[TAILLE_TAMPON];
    struct sockaddr_in adresse_expediteur;
    socklen_t taille_adresse = sizeof(adresse_expediteur);

    while (1) {
        int octets_recus = recvfrom(descripteur_socket, message_recu, TAILLE_TAMPON, 0, (struct sockaddr *)&adresse_expediteur, &taille_adresse);
        if (octets_recus < 0) continue;
        message_recu[octets_recus] = '\0';
        char *ip_expediteur = inet_ntoa(adresse_expediteur.sin_addr);

        /* 2.1 - GESTION DES INSCRIPTIONS (JOIN) */
        if (strcmp(message_recu, "JOIN-T-PMI") == 0) {
            pthread_mutex_lock(&verrou_partage);
            strcpy(liste_terminaux_locaux[nombre_terminaux_locaux++], ip_expediteur);
            pthread_mutex_unlock(&verrou_partage);

            struct sockaddr_in dest_racine = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
            inet_pton(AF_INET, ip_racine_pmr, &dest_racine.sin_addr);
            sendto(descripteur_socket, "JOIN-PMI-PMR", 12, 0, (struct sockaddr *)&dest_racine, sizeof(dest_racine));
        }
        else if (strcmp(message_recu, "JOIN-PMI-PMR") == 0 && je_suis_la_racine) {
            pthread_mutex_lock(&verrou_partage);
            // On enregistre la PMI et on estime son coût à 1 (ou selon topologie si connue)
            strcpy(liste_voisins_multicast[nombre_voisins_multicast], ip_expediteur);
            couts_voisins_multicast[nombre_voisins_multicast] = 1;
            nombre_voisins_multicast++;

            // CONSTRUCTION DU JACK LISTE (Harmonisé avec le bloc 2.2)
            char jack_annonce[TAILLE_TAMPON];
            sprintf(jack_annonce, "JACK|");
            // On ajoute la Racine elle-même dans la liste
            sprintf(jack_annonce + strlen(jack_annonce), "%s,0;", mon_ip);
            // On ajoute tous les membres actuels
            for(int i=0; i < nombre_voisins_multicast; i++) {
                sprintf(jack_annonce + strlen(jack_annonce), "%s,%d;",
                        liste_voisins_multicast[i], couts_voisins_multicast[i]);
            }
            pthread_mutex_unlock(&verrou_partage);

            // Envoi du JACK à tout le monde
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                struct sockaddr_in client_arbre = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                inet_pton(AF_INET, liste_voisins_multicast[i], &client_arbre.sin_addr);
                sendto(descripteur_socket, jack_annonce, strlen(jack_annonce), 0, (struct sockaddr *)&client_arbre, sizeof(client_arbre));
            }
        }
        else if (strcmp(message_recu, "JOIN-PMI-PMI") == 0) {
            pthread_mutex_lock(&verrou_partage);
            strcpy(liste_voisins_multicast[nombre_voisins_multicast++], ip_expediteur);
            pthread_mutex_unlock(&verrou_partage);
        }

        /* 2.2 - RECALCUL DYNAMIQUE LORS D'UN JACK (Optimisation 2 sens) */
        else if (strncmp(message_recu, "JACK", 4) == 0) {
            char copie_message[TAILLE_TAMPON];
            strcpy(copie_message, message_recu + 5);

            char *segment_noeud;
            char *sauvegarde_ptr;
            segment_noeud = strtok_r(copie_message, ";", &sauvegarde_ptr);

            while (segment_noeud != NULL) {
                char ip_potentielle[16];
                int dist_racine_vers_potentiel;
                sscanf(segment_noeud, "%[^,],%d", ip_potentielle, &dist_racine_vers_potentiel);

                if (strcmp(ip_potentielle, mon_ip) != 0) {
                    int cout_physique = 999;
                    for(int i = 0; i < nombre_voisins_physiques; i++) {
                        if(strcmp(topologie_reseau[i].adresse_ip, ip_potentielle) == 0) {
                            cout_physique = topologie_reseau[i].cout_direct;
                        }
                    }

                    pthread_mutex_lock(&verrou_partage);
                    if ((dist_racine_vers_potentiel + cout_physique) < mon_cout_actuel_vers_racine) {
                        char ancien_parent[16];
                        strcpy(ancien_parent, ip_de_mon_parent_actuel);
                        strcpy(ip_de_mon_parent_actuel, ip_potentielle);
                        mon_cout_actuel_vers_racine = dist_racine_vers_potentiel + cout_physique;
                        pthread_mutex_unlock(&verrou_partage);

                        struct sockaddr_in adresse_controle = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                        inet_pton(AF_INET, ip_de_mon_parent_actuel, &adresse_controle.sin_addr);
                        sendto(descripteur_socket, "JOIN-PMI-PMI", 12, 0, (struct sockaddr *)&adresse_controle, sizeof(adresse_controle));

                        inet_pton(AF_INET, ancien_parent, &adresse_controle.sin_addr);
                        sendto(descripteur_socket, "PRUNE-PMI-PMI", 13, 0, (struct sockaddr *)&adresse_controle, sizeof(adresse_controle));
                    } else {
                        pthread_mutex_unlock(&verrou_partage);
                    }
                }
                segment_noeud = strtok_r(NULL, ";", &sauvegarde_ptr);
            }
        }

        /* 2.3 - ÉLAGAGE (PRUNE) */
        else if (strncmp(message_recu, "PRUNE", 5) == 0) {
            pthread_mutex_lock(&verrou_partage);
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                if (strcmp(liste_voisins_multicast[i], ip_expediteur) == 0) {
                    strcpy(liste_voisins_multicast[i], liste_voisins_multicast[--nombre_voisins_multicast]);
                    break;
                }
            }

            if (nombre_voisins_multicast == 0 && nombre_terminaux_locaux == 0 && !je_suis_la_racine) {
                struct sockaddr_in parent = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                inet_pton(AF_INET, ip_de_mon_parent_actuel, &parent.sin_addr);
                sendto(descripteur_socket, "PRUNE-PMI-PMI", 13, 0, (struct sockaddr *)&parent, sizeof(parent));
            }
            pthread_mutex_unlock(&verrou_partage);
        }
    }
}

/******************************************************************************
 * BLOC 3 : PLAN DE DONNÉES (DUPLICATION DU FLUX)
 *****************************************************************************/
void* thread_donnees(void* arg) {
    int socket_donnees = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in adresse_flux = { .sin_family = AF_INET, .sin_port = htons(PORT_DONNEES), .sin_addr.s_addr = INADDR_ANY };
    bind(socket_donnees, (struct sockaddr *)&adresse_flux, sizeof(adresse_flux));

    char tampon_donnees[TAILLE_TAMPON];
    struct sockaddr_in destination_envoi = { .sin_family = AF_INET, .sin_port = htons(PORT_DONNEES) };

    while (1) {
        int taille_paquet = recv(socket_donnees, tampon_donnees, TAILLE_TAMPON, 0);
        if (taille_paquet <= 0) continue;

        pthread_mutex_lock(&verrou_partage);
        for (int i = 0; i < nombre_terminaux_locaux; i++) {
            inet_pton(AF_INET, liste_terminaux_locaux[i], &destination_envoi.sin_addr);
            sendto(socket_donnees, tampon_donnees, taille_paquet, 0, (struct sockaddr *)&destination_envoi, sizeof(destination_envoi));
        }
        for (int i = 0; i < nombre_voisins_multicast; i++) {
            inet_pton(AF_INET, liste_voisins_multicast[i], &destination_envoi.sin_addr);
            sendto(socket_donnees, tampon_donnees, taille_paquet, 0, (struct sockaddr *)&destination_envoi, sizeof(destination_envoi));
        }
        pthread_mutex_unlock(&verrou_partage);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) { printf("Usage: %s <Mon_IP_Locale> <config.json>\n", argv[0]); return 1; }
    strcpy(mon_ip, argv[1]);
    charger_configuration(argv[2]);

    pthread_t fil_controle, fil_donnees;
    pthread_create(&fil_controle, NULL, thread_controle, NULL);
    pthread_create(&fil_donnees, NULL, thread_donnees, NULL);

    printf("[SYSTEME] Passerelle lancée sur %s (%s)\n", mon_ip, je_suis_la_racine ? "PMR" : "PMI");
    pthread_join(fil_controle, NULL);
    pthread_join(fil_donnees, NULL);
    return 0;
}