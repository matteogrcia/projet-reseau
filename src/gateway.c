#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
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
int mon_cout_actuel_vers_racine = 999;
char ip_de_mon_parent_actuel[16];

VoisinPhysique topologie_reseau[NOMBRE_MAX_NOEUDS];
int nombre_voisins_physiques = 0;

char liste_voisins_multicast[NOMBRE_MAX_NOEUDS][16];
int nombre_voisins_multicast = 0;
int couts_voisins_multicast[NOMBRE_MAX_NOEUDS];
char liste_terminaux_locaux[NOMBRE_MAX_NOEUDS][16];
int nombre_terminaux_locaux = 0;

// Compteur de séquence pour les paquets de données
unsigned long compteur_sequence_data = 0;

pthread_mutex_t verrou_partage = PTHREAD_MUTEX_INITIALIZER;

/* --- UTILITAIRE D'HORODATAGE REQUIS --- */
void obtenir_heure_hm(char *tampon, size_t taille) {
    time_t maintenant = time(NULL);
    struct tm *temps_local = localtime(&maintenant);
    strftime(tampon, taille, "%H:%M", temps_local);
}

/* --- DIAGNOSTIC MANUEL (SIGUSR1 via show_tree.sh) --- */
void gestionnaire_signal_arbre(int sig) {
    char hm[10];
    obtenir_heure_hm(hm, sizeof(hm));

    pthread_mutex_lock(&verrou_partage);
    printf("\n*************************************************\n");
    printf("%s          DIAGNOSTIC                           --\n", hm);
    printf("*************************************************\n");
    printf(" Mode Passerelle        : %s\n", je_suis_la_racine ? "RACINE (PMR)" : "INTERMÉDIAIRE (PMI)");
    printf(" IP Déclarée            : %s\n", mon_ip);
    printf(" Parent Actuel          : %s\n", ip_de_mon_parent_actuel);
    printf(" Coût cumulé à la racine: %d\n", mon_cout_actuel_vers_racine);
    printf(" Enfants Multicast (%d) : ", nombre_voisins_multicast);
    for(int i = 0; i < nombre_voisins_multicast; i++) printf("[%s (coût: %d)] ", liste_voisins_multicast[i], couts_voisins_multicast[i]);
    printf("\n Terminaux Locaux  (%d) : ", nombre_terminaux_locaux);
    for(int i = 0; i < nombre_terminaux_locaux; i++) printf("[%s] ", liste_terminaux_locaux[i]);
    printf("\n*************************************************\n\n");
    pthread_mutex_unlock(&verrou_partage);
}

/******************************************************************************
 * BLOC 1 : CONFIGURATION & PARSING
 *****************************************************************************/
void charger_configuration(const char *nom_fichier) {
    char hm[10];
    obtenir_heure_hm(hm, sizeof(hm));

    printf("[%s] [INIT] Lecture du fichier de configuration : %s\n", hm, nom_fichier);
    FILE *fichier = fopen(nom_fichier, "rb");
    if (!fichier) { perror("Erreur d'ouverture du JSON"); exit(1); }

    fseek(fichier, 0, SEEK_END);
    long longueur = ftell(fichier);
    fseek(fichier, 0, SEEK_SET);
    char *donnees_json = malloc(longueur + 1);
    fread(donnees_json, 1, longueur, fichier);
    fclose(fichier);

    cJSON *objet_json = cJSON_Parse(donnees_json);
    if (!objet_json) { printf("Erreur de parsing cJSON !\n"); exit(1); }

    strcpy(ip_racine_pmr, cJSON_GetObjectItem(objet_json, "root_gateway")->valuestring);
    printf("[%s] [INIT] IP de la Racine (PMR) déclarée : %s\n", hm, ip_racine_pmr);

    if (strcmp(mon_ip, ip_racine_pmr) == 0) {
        je_suis_la_racine = 1;
        mon_cout_actuel_vers_racine = 0;
        printf("[%s] [INIT] Égalité détectée ! Je me positionne comme RACINE (PMR).\n", hm);
    } else {
        printf("[%s] [INIT] Je suis une passerelle INTERMÉDIAIRE (PMI).\n", hm);
    }

    cJSON *passerelles = cJSON_GetObjectItem(objet_json, "gateways");
    int taille = cJSON_GetArraySize(passerelles);
    printf("[%s] [INIT] Chargement GLOBAL de la topologie réseau (%d noeuds) :\n", hm, taille);

    for (int i = 0; i < taille; i++) {
        cJSON *item = cJSON_GetArrayItem(passerelles, i);
        const char* id = cJSON_GetObjectItem(item, "id")->valuestring;
        const char* ip = cJSON_GetObjectItem(item, "ip")->valuestring;
        cJSON *cost_item = cJSON_GetObjectItem(item, "cost");
        int cout = cost_item ? cost_item->valueint : 1;

        strcpy(topologie_reseau[nombre_voisins_physiques].adresse_ip, ip);
        topologie_reseau[nombre_voisins_physiques].cout_direct = cout;
        printf("       -> [%s] IP: %s | Coût Physique Direct: %d\n", id, ip, cout);
        nombre_voisins_physiques++;
    }

    strcpy(ip_de_mon_parent_actuel, ip_racine_pmr);
    if (!je_suis_la_racine) {
        for (int i = 0; i < nombre_voisins_physiques; i++) {
            if (strcmp(topologie_reseau[i].adresse_ip, ip_racine_pmr) == 0) {
                mon_cout_actuel_vers_racine = topologie_reseau[i].cout_direct;
                printf("[%s] [INIT] Coût initial calqué sur la racine %s = %d\n", hm, ip_racine_pmr, mon_cout_actuel_vers_racine);
                break;
            }
        }
    }

    free(donnees_json);
    cJSON_Delete(objet_json);
    printf("[%s] [INIT] Configuration chargée avec succès.\n\n", hm);
}

/******************************************************************************
 * BLOC 2 : SOURCE DE FLUX INTERNE (DÉSACTIVÉ PAR TON IF DE MAIN)
 *****************************************************************************/
void* thread_source_flux(void* arg) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in dest = { .sin_family = AF_INET, .sin_port = htons(PORT_DONNEES) };
    inet_pton(AF_INET, "127.0.0.1", &dest.sin_addr);

    int cpt = 0;
    char msg[128];
    while (1) {
        sprintf(msg, "PAQUET_MULTICAST_%d", cpt++);
        sendto(sock, msg, strlen(msg), 0, (struct sockaddr *)&dest, sizeof(dest));
        sleep(2);
    }
    return NULL;
}

/******************************************************************************
 * BLOC 3 : PLAN DE CONTRÔLE (SIGNALISATION & ALGORITHME JACK)
 *****************************************************************************/
void* thread_controle(void* arg) {
    char hm[10];
    obtenir_heure_hm(hm, sizeof(hm));
    printf("[%s] [CONTROL] Thread de signalisation actif sur le Port %d\n", hm, PORT_CONTROLE);

    int ds = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in ecoute = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE), .sin_addr.s_addr = INADDR_ANY };
    bind(ds, (const struct sockaddr *)&ecoute, sizeof(ecoute));

    char buf[TAILLE_TAMPON];
    struct sockaddr_in exp;
    socklen_t len = sizeof(exp);

    while (1) {
        int n = recvfrom(ds, buf, TAILLE_TAMPON, 0, (struct sockaddr *)&exp, &len);
        if (n < 0) continue;
        buf[n] = '\0';
        char ip_exp[16];
        inet_ntop(AF_INET, &exp.sin_addr, ip_exp, 16);

        obtenir_heure_hm(hm, sizeof(hm));

        /* 3.1 - ENREGISTREMENTS (JOINS) */
        if (strcmp(buf, "JOIN-T-PMI") == 0) {
            printf("\n*************************************************\n");
            printf("%s          JOIN-T-PMI                           --\n", hm);
            printf("*************************************************\n");
            printf("📩 Message reçu de %s\n", ip_exp);

            pthread_mutex_lock(&verrou_partage);
            int deja_present = 0;
            for (int i = 0; i < nombre_terminaux_locaux; i++) {
                if (strcmp(liste_terminaux_locaux[i], ip_exp) == 0) { deja_present = 1; break; }
            }
            if (!deja_present) {
                strcpy(liste_terminaux_locaux[nombre_terminaux_locaux++], ip_exp);
                printf("   Terminal local enregistré : %s (Total locaux = %d)\n", ip_exp, nombre_terminaux_locaux);
            } else {
                printf("   Terminal local %s déjà enregistré.\n", ip_exp);
            }
            pthread_mutex_unlock(&verrou_partage);

            printf("    [RELAY] Envoi immédiat de 'JOIN-PMI-PMR' vers la racine %s\n", ip_racine_pmr);
            struct sockaddr_in r = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
            inet_pton(AF_INET, ip_racine_pmr, &r.sin_addr);
            sendto(ds, "JOIN-PMI-PMR", 12, 0, (struct sockaddr *)&r, sizeof(r));
        }

        else if (strcmp(buf, "JOIN-PMI-PMR") == 0) {
            if (!je_suis_la_racine) {
                continue;
            }
            printf("\n*************************************************\n");
            printf("%s          JOIN-PMI-PMR                         --\n", hm);
            printf("*************************************************\n");
            printf("📩 Message reçu de %s\n", ip_exp);

            pthread_mutex_lock(&verrou_partage);
            int deja_present = 0;
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) { deja_present = 1; break; }
            }
            if (!deja_present) {
                strcpy(liste_voisins_multicast[nombre_voisins_multicast], ip_exp);

                int cout_physique_associe = 1;
                for (int j = 0; j < nombre_voisins_physiques; j++) {
                    if (strcmp(topologie_reseau[j].adresse_ip, ip_exp) == 0) {
                        cout_physique_associe = topologie_reseau[j].cout_direct;
                        break;
                    }
                }
                couts_voisins_multicast[nombre_voisins_multicast] = cout_physique_associe;
                printf("   ➕ Passerelle enfant ajoutée : %s | Coût physique associé = %d\n", ip_exp, cout_physique_associe);
                nombre_voisins_multicast++;
            }

            char jack[TAILLE_TAMPON];
            sprintf(jack, "JACK|%s,0;", mon_ip);
            for(int i=0; i < nombre_voisins_multicast; i++) {
                sprintf(jack + strlen(jack), "%s,%d;", liste_voisins_multicast[i], couts_voisins_multicast[i]);
            }
            pthread_mutex_unlock(&verrou_partage);

            printf("    [BROADCAST-JACK] Envoi de l'arbre global : %s\n", jack);
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                struct sockaddr_in c = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                inet_pton(AF_INET, liste_voisins_multicast[i], &c.sin_addr);
                sendto(ds, jack, strlen(jack), 0, (struct sockaddr *)&c, sizeof(c));
            }
        }

        else if (strcmp(buf, "JOIN-PMI-PMI") == 0) {
            printf("\n*************************************************\n");
            printf("%s          JOIN-PMI-PMI                         --\n", hm);
            printf("*************************************************\n");
            printf(" Message reçu de %s\n", ip_exp);

            pthread_mutex_lock(&verrou_partage);
            int deja_present = 0;
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) { deja_present = 1; break; }
            }
            if (!deja_present) {
                strcpy(liste_voisins_multicast[nombre_voisins_multicast], ip_exp);
                couts_voisins_multicast[nombre_voisins_multicast] = 1;
                nombre_voisins_multicast++;
                printf("   Une passerelle voisine %s s'est greffée sur moi.\n", ip_exp);
            }
            pthread_mutex_unlock(&verrou_partage);
        }

        /* 3.2 - ALGORITHME DE RECALCUL DES CHEMINS (JACK) */
        else if (strncmp(buf, "JACK", 4) == 0) {
            printf("\n*************************************************\n");
            printf("%s          JACK                                 --\n", hm);
            printf("*************************************************\n");
            printf(" Message reçu de %s : \"%s\"\n", ip_exp, buf);
            printf("    Analyse du vecteur d'arbre...\n");

            char copie[TAILLE_TAMPON];
            strcpy(copie, buf + 5);
            char *seg, *ptr;
            seg = strtok_r(copie, ";", &ptr);

            while (seg != NULL) {
                char ip_p[16];
                int d_r;
                if (sscanf(seg, "%[^,],%d", ip_p, &d_r) == 2) {
                    printf("      ▪️ Segment distant: %s | Distance déclarée à la racine: %d\n", ip_p, d_r);

                    if (strcmp(ip_p, mon_ip) != 0) {
                        int c_p = 999;
                        for(int i = 0; i < nombre_voisins_physiques; i++) {
                            if(strcmp(topologie_reseau[i].adresse_ip, ip_p) == 0) {
                                c_p = topologie_reseau[i].cout_direct;
                                break;
                            }
                        }

                        printf("          Coût physique direct obtenu vers %s = %d\n", ip_p, c_p);
                        int score_total = d_r + c_p;
                        printf("          Évaluation : %d (vecteur) + %d (lien local) = %d\n", d_r, c_p, score_total);

                        pthread_mutex_lock(&verrou_partage);

                        if (score_total <= mon_cout_actuel_vers_racine) {
                            char old[16];
                            strcpy(old, ip_de_mon_parent_actuel);

                            if (score_total == mon_cout_actuel_vers_racine && strcmp(old, ip_p) != 0) {
                                pthread_mutex_unlock(&verrou_partage);
                                printf("          Chemin rejeté (Coût identique mais parent différent, conservation de la stabilité).\n");
                                seg = strtok_r(NULL, ";", &ptr);
                                continue;
                            }

                            strcpy(ip_de_mon_parent_actuel, ip_p);
                            mon_cout_actuel_vers_racine = score_total;
                            pthread_mutex_unlock(&verrou_partage);

                            if (strcmp(old, ip_p) != 0) {
                                printf("          [RECALCUL-SUCCÈS] Raccourci validé ! Nouveau parent: %s | Coût: %d\n", ip_p, mon_cout_actuel_vers_racine);

                                printf("          Send 'JOIN-PMI-PMI' au nouveau parent %s\n", ip_p);
                                struct sockaddr_in dest = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                                inet_pton(AF_INET, ip_de_mon_parent_actuel, &dest.sin_addr);
                                sendto(ds, "JOIN-PMI-PMI", 12, 0, (struct sockaddr *)&dest, sizeof(dest));

                                inet_pton(AF_INET, old, &dest.sin_addr);
                                const char* m_prune = (strcmp(old, ip_racine_pmr) == 0) ? "PRUNE-PMI-PMR" : "PRUNE-PMI-PMI";
                                printf("          Send Elagage '%s' à l'ancien parent %s\n", m_prune, old);
                                sendto(ds, m_prune, strlen(m_prune), 0, (struct sockaddr *)&dest, sizeof(dest));
                            } else {
                                printf("          [MAJ-STABLE] Chemin maintenu optimal via le parent actuel %s (Coût: %d)\n", ip_p, mon_cout_actuel_vers_racine);
                            }
                        } else {
                            printf("          Chemin rejeté (Coût sous-optimal : %d > %d).\n", score_total, mon_cout_actuel_vers_racine);
                            pthread_mutex_unlock(&verrou_partage);
                        }
                    } else {
                        printf("          Adresse locale ignorée (prévention de boucle).\n");
                    }
                }
                seg = strtok_r(NULL, ";", &ptr);
            }
            printf("    Fin de l'analyse du message JACK.\n");
        }

        /* 3.3 - DÉSENREGISTREMENT / ÉLAGAGE (PRUNE) */
        else if (strncmp(buf, "PRUNE", 5) == 0) {
            printf("\n*************************************************\n");
            printf("%s          PRUNE                                --\n", hm);
            printf("*************************************************\n");
            printf(" Message reçu de %s : \"%s\"\n", ip_exp, buf);

            pthread_mutex_lock(&verrou_partage);
            if (strcmp(buf, "PRUNE-T-PMI") == 0 || strcmp(buf, "PRUNE-T-PMR") == 0) {
                for (int i = 0; i < nombre_terminaux_locaux; i++) {
                    if (strcmp(liste_terminaux_locaux[i], ip_exp) == 0) {
                        strcpy(liste_terminaux_locaux[i], liste_terminaux_locaux[--nombre_terminaux_locaux]);
                        printf("    Terminal local %s retiré. Restants: %d\n", ip_exp, nombre_terminaux_locaux);
                        i--;
                    }
                }
            }
            else if (strcmp(buf, "PRUNE-PMI-PMI") == 0 || strcmp(buf, "PRUNE-PMI-PMR") == 0) {
                for (int i = 0; i < nombre_voisins_multicast; i++) {
                    if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) {
                        strcpy(liste_voisins_multicast[i], liste_voisins_multicast[--nombre_voisins_multicast]);
                        printf("    Passerelle enfant %s retirée. Restantes: %d\n", ip_exp, nombre_voisins_multicast);
                        i--;
                    }
                }
            }

            if (nombre_voisins_multicast == 0 && nombre_terminaux_locaux == 0 && !je_suis_la_racine) {
                printf("    [AUTO-PRUNE] Aucun récepteur dépendant. Auto-élagage de l'arbre.\n");
                struct sockaddr_in p = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                inet_pton(AF_INET, ip_de_mon_parent_actuel, &p.sin_addr);
                const char* msg = (strcmp(ip_de_mon_parent_actuel, ip_racine_pmr) == 0) ? "PRUNE-PMI-PMR" : "PRUNE-PMI-PMI";
                printf("    Envoi de %s vers mon parent %s\n", msg, ip_de_mon_parent_actuel);
                sendto(ds, msg, strlen(msg), 0, (struct sockaddr *)&p, sizeof(p));
            }
            pthread_mutex_unlock(&verrou_partage);
        }
    }
}

/******************************************************************************
 * BLOC 4 : PLAN DE DONNÉES (DUPLICATION EN MODE SANS RESTRICTION)
 *****************************************************************************/
void* thread_donnees(void* arg) {
    char hm[10];
    obtenir_heure_hm(hm, sizeof(hm));
    printf("[%s] [DATA] Thread de transmission de données actif sur le Port %d\n", hm, PORT_DONNEES);

    int sd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in f = { .sin_family = AF_INET, .sin_port = htons(PORT_DONNEES), .sin_addr.s_addr = INADDR_ANY };
    bind(sd, (struct sockaddr *)&f, sizeof(f));

    char t[TAILLE_TAMPON];
    struct sockaddr_in d = { .sin_family = AF_INET, .sin_port = htons(PORT_DONNEES) };
    struct sockaddr_in exp_data;
    socklen_t len_exp = sizeof(exp_data);

    while (1) {
        int sz = recvfrom(sd, t, TAILLE_TAMPON, 0, (struct sockaddr *)&exp_data, &len_exp);
        if (sz <= 0) continue;

        char ip_source_paquet[16];
        inet_ntop(AF_INET, &exp_data.sin_addr, ip_source_paquet, 16);

        pthread_mutex_lock(&verrou_partage);

        // Incrémentation du numéro de séquence à chaque réception de paquet de données valide
        compteur_sequence_data++;
        obtenir_heure_hm(hm, sizeof(hm));

        // Format de log exigé par le professeur
        printf("\n*************************************************\n");
        printf("%s          DATA                                 %lu\n", hm, compteur_sequence_data);
        printf("*************************************************\n");
        printf(" Flux reçu de l'émetteur : %s | Taille : %d octets\n", ip_source_paquet, sz);

        // 1. Relai vers les Terminaux Locaux branchés sur cette PMI
        for (int i = 0; i < nombre_terminaux_locaux; i++) {
            printf("   ↳  Relai -> Terminal Local : %s\n", liste_terminaux_locaux[i]);
            inet_pton(AF_INET, liste_terminaux_locaux[i], &d.sin_addr);
            sendto(sd, t, sz, 0, (struct sockaddr *)&d, sizeof(d));
        }

        // 2. Relai vers les Passerelles Enfants Multicast (Descente d'arbre)
        for (int i = 0; i < nombre_voisins_multicast; i++) {
            if (strcmp(liste_voisins_multicast[i], ip_source_paquet) != 0) {
                printf("   ↳ Relai -> Passerelle Enfant : %s\n", liste_voisins_multicast[i]);
                inet_pton(AF_INET, liste_voisins_multicast[i], &d.sin_addr);
                sendto(sd, t, sz, 0, (struct sockaddr *)&d, sizeof(d));
            }
        }
        pthread_mutex_unlock(&verrou_partage);
    }
}

/******************************************************************************
 * POINT D'ENTRÉE PRINCIPAL
 *****************************************************************************/
int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf(" Usage incorrect ! Exemple : ./gateway <IP_LOCALE> <CONFIG_JSON>\n");
        return 1;
    }

    signal(SIGUSR1, gestionnaire_signal_arbre);

    strcpy(mon_ip, argv[1]);
    charger_configuration(argv[2]);

    pthread_t c, d, s;
    pthread_create(&c, NULL, thread_controle, NULL);
    pthread_create(&d, NULL, thread_donnees, NULL);

    char hm[10];
    obtenir_heure_hm(hm, sizeof(hm));
    printf("[%s] [SYSTEM-READY] Passerelle en ligne : %s (%s)\n\n",
           hm, je_suis_la_racine ? "PMR (Racine)" : "PMI (Intermédiaire)", mon_ip);

    pthread_join(c, NULL);
    pthread_join(d, NULL);
    if (je_suis_la_racine) pthread_join(s, NULL);

    return 0;
}