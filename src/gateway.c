#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
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

pthread_mutex_t verrou_partage = PTHREAD_MUTEX_INITIALIZER;

/* --- DIAGNOSTIC MANUEL (SIGUSR1 via show_tree.sh) --- */
void gestionnaire_signal_arbre(int sig) {
    pthread_mutex_lock(&verrou_partage);
    printf("\n=================================================\n");
    printf(" 📊 [SIGUSR1] DIAGNOSTIC DEMANDÉ PAR L'UTILISATEUR\n");
    printf(" Mode Passerelle        : %s\n", je_suis_la_racine ? "RACINE (PMR)" : "INTERMÉDIAIRE (PMI)");
    printf(" IP Déclarée            : %s\n", mon_ip);
    printf(" Parent Actuel          : %s\n", ip_de_mon_parent_actuel);
    printf(" Coût cumulé à la racine: %d\n", mon_cout_actuel_vers_racine);
    printf(" Enfants Multicast (%d) : ", nombre_voisins_multicast);
    for(int i = 0; i < nombre_voisins_multicast; i++) printf("[%s (coût: %d)] ", liste_voisins_multicast[i], couts_voisins_multicast[i]);
    printf("\n Terminaux Locaux  (%d) : ", nombre_terminaux_locaux);
    for(int i = 0; i < nombre_terminaux_locaux; i++) printf("[%s] ", liste_terminaux_locaux[i]);
    printf("\n=================================================\n\n");
    pthread_mutex_unlock(&verrou_partage);
}

/******************************************************************************
 * BLOC 1 : CONFIGURATION & PARSING
 *****************************************************************************/
void charger_configuration(const char *nom_fichier) {
    printf("[INIT] Lecture du fichier de configuration : %s\n", nom_fichier);
    FILE *fichier = fopen(nom_fichier, "rb");
    if (!fichier) { perror("❌ Erreur d'ouverture du JSON"); exit(1); }

    fseek(fichier, 0, SEEK_END);
    long longueur = ftell(fichier);
    fseek(fichier, 0, SEEK_SET);
    char *donnees_json = malloc(longueur + 1);
    fread(donnees_json, 1, longueur, fichier);
    fclose(fichier);

    cJSON *objet_json = cJSON_Parse(donnees_json);
    if (!objet_json) { printf("❌ Erreur de parsing cJSON !\n"); exit(1); }

    strcpy(ip_racine_pmr, cJSON_GetObjectItem(objet_json, "root_gateway")->valuestring);
    printf("[INIT] IP de la Racine (PMR) déclarée : %s\n", ip_racine_pmr);

    if (strcmp(mon_ip, ip_racine_pmr) == 0) {
        je_suis_la_racine = 1;
        mon_cout_actuel_vers_racine = 0;
        printf("[INIT] Égalité détectée ! Je me positionne comme RACINE (PMR).\n");
    } else {
        printf("[INIT] Je suis une passerelle INTERMÉDIAIRE (PMI).\n");
    }

    cJSON *passerelles = cJSON_GetObjectItem(objet_json, "gateways");
    int taille = cJSON_GetArraySize(passerelles);
    printf("[INIT] Chargement GLOBAL de la topologie réseau depuis le JSON (%d noeuds) :\n", taille);

    // FIX : On stocke tous les nœuds sans exception pour garantir la résolution des coûts
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
                printf("[INIT] Coût initial calqué sur le JSON vers la racine %s = %d\n", ip_racine_pmr, mon_cout_actuel_vers_racine);
                break;
            }
        }
    }

    free(donnees_json);
    cJSON_Delete(objet_json);
    printf("[INIT] Configuration chargée avec succès.\n\n");
}

/******************************************************************************
 * BLOC 2 : SOURCE DE FLUX INTERNE (PMR SEULEMENT)
 *****************************************************************************/
void* thread_source_flux(void* arg) {
    printf("[FLUX-SRC] Démarrage du générateur de flux multicast fictif (127.0.0.1)\n");
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
    printf("[CONTROL] Thread de signalisation actif sur le Port %d\n", PORT_CONTROLE);
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

        printf("\n📩 [CONTROL] Message reçu de %s : \"%s\"\n", ip_exp, buf);

        /* 3.1 - ENREGISTREMENTS (JOINS) */
        if (strcmp(buf, "JOIN-T-PMI") == 0) {
            pthread_mutex_lock(&verrou_partage);
            int deja_present = 0;
            for (int i = 0; i < nombre_terminaux_locaux; i++) {
                if (strcmp(liste_terminaux_locaux[i], ip_exp) == 0) { deja_present = 1; break; }
            }
            if (!deja_present) {
                strcpy(liste_terminaux_locaux[nombre_terminaux_locaux++], ip_exp);
                printf("   ➕ [JOIN-T] Terminal local enregistré : %s (Total locaux = %d)\n", ip_exp, nombre_terminaux_locaux);
            } else {
                printf("   ℹ️ [JOIN-T] Terminal local %s déjà enregistré.\n", ip_exp);
            }
            pthread_mutex_unlock(&verrou_partage);

            printf("   ➡️ [RELAY] Envoi immédiat de 'JOIN-PMI-PMR' vers la racine %s\n", ip_racine_pmr);
            struct sockaddr_in r = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
            inet_pton(AF_INET, ip_racine_pmr, &r.sin_addr);
            sendto(ds, "JOIN-PMI-PMR", 12, 0, (struct sockaddr *)&r, sizeof(r));
        }

        else if (strcmp(buf, "JOIN-PMI-PMR") == 0) {
            if (!je_suis_la_racine) {
                printf("   ⚠️ [JOIN-WARN] Reçu JOIN-PMI-PMR mais je ne suis pas la racine. Ignoré.\n");
                continue;
            }
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
                printf("   ➕ [JOIN-PMR] Passerelle enfant ajoutée : %s | Coût physique associé = %d\n", ip_exp, cout_physique_associe);
                nombre_voisins_multicast++;
            }

            // Construction de la chaîne JACK complète
            char jack[TAILLE_TAMPON];
            sprintf(jack, "JACK|%s,0;", mon_ip);
            for(int i=0; i < nombre_voisins_multicast; i++) {
                sprintf(jack + strlen(jack), "%s,%d;", liste_voisins_multicast[i], couts_voisins_multicast[i]);
            }
            pthread_mutex_unlock(&verrou_partage);

            // Diffusion du paquet JACK à tout l'arbre
            printf("   📢 [BROADCAST-JACK] Envoi de l'arbre global : %s\n", jack);
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                struct sockaddr_in c = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                inet_pton(AF_INET, liste_voisins_multicast[i], &c.sin_addr);
                sendto(ds, jack, strlen(jack), 0, (struct sockaddr *)&c, sizeof(c));
            }
        }

        else if (strcmp(buf, "JOIN-PMI-PMI") == 0) {
            pthread_mutex_lock(&verrou_partage);
            int deja_present = 0;
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) { deja_present = 1; break; }
            }
            if (!deja_present) {
                strcpy(liste_voisins_multicast[nombre_voisins_multicast], ip_exp);
                couts_voisins_multicast[nombre_voisins_multicast] = 1; // par défaut
                nombre_voisins_multicast++;
                printf("   ➕ [JOIN-PMI] Une passerelle voisine %s s'est greffée sur moi.\n", ip_exp);
            }
            pthread_mutex_unlock(&verrou_partage);
        }

        /* 3.2 - ALGORITHME DE RECALCUL DES CHEMINS (JACK) */
        else if (strncmp(buf, "JACK", 4) == 0) {
            printf("   🔍 [ALGO-JACK] Début de l'analyse du vecteur d'arbre...\n");
            char copie[TAILLE_TAMPON];
            strcpy(copie, buf + 5);
            char *seg, *ptr;
            seg = strtok_r(copie, ";", &ptr);

            while (seg != NULL) {
                char ip_p[16];
                int d_r;
                if (sscanf(seg, "%[^,],%d", ip_p, &d_r) == 2) {
                    printf("      ▪️ Segment extrait -> Noeud distant: %s | Distance déclarée à la racine: %d\n", ip_p, d_r);

                    if (strcmp(ip_p, mon_ip) != 0) {
                        int c_p = 999;
                        for(int i = 0; i < nombre_voisins_physiques; i++) {
                            if(strcmp(topologie_reseau[i].adresse_ip, ip_p) == 0) {
                                c_p = topologie_reseau[i].cout_direct;
                                break;
                            }
                        }

                        printf("         👉 Coût physique direct obtenu vers %s = %d\n", ip_p, c_p);
                        int score_total = d_r + c_p;
                        printf("         👉 Évaluation du chemin : %d (via vecteur) + %d (lien local) = %d\n", d_r, c_p, score_total);

                        pthread_mutex_lock(&verrou_partage);

                        // FIX: Utilisation de <= pour actualiser les arbres ou valider les raccourcis
                        if (score_total <= mon_cout_actuel_vers_racine) {
                            char old[16];
                            strcpy(old, ip_de_mon_parent_actuel);

                            // Si le coût est identique mais que l'émetteur est différent du parent actuel, on évite les oscillations inutiles
                            if (score_total == mon_cout_actuel_vers_racine && strcmp(old, ip_p) != 0) {
                                pthread_mutex_unlock(&verrou_partage);
                                printf("         ❌ Chemin rejeté (Coût identique mais parent différent, conservation de la stabilité).\n");
                                seg = strtok_r(NULL, ";", &ptr);
                                continue;
                            }

                            strcpy(ip_de_mon_parent_actuel, ip_p);
                            mon_cout_actuel_vers_racine = score_total;
                            pthread_mutex_unlock(&verrou_partage);

                            if (strcmp(old, ip_p) != 0) {
                                printf("         🚀 [RECALCUL-SUCCÈS] Raccourci ou changement validé ! Nouveau parent: %s | Coût total: %d\n", ip_p, mon_cout_actuel_vers_racine);

                                printf("         ➡️ Send 'JOIN-PMI-PMI' au nouveau parent %s\n", ip_p);
                                struct sockaddr_in dest = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                                inet_pton(AF_INET, ip_de_mon_parent_actuel, &dest.sin_addr);
                                sendto(ds, "JOIN-PMI-PMI", 12, 0, (struct sockaddr *)&dest, sizeof(dest));

                                inet_pton(AF_INET, old, &dest.sin_addr);
                                const char* m_prune = (strcmp(old, ip_racine_pmr) == 0) ? "PRUNE-PMI-PMR" : "PRUNE-PMI-PMI";
                                printf("         🪓 Send Elagage '%s' à l'ancien parent %s\n", m_prune, old);
                                sendto(ds, m_prune, strlen(m_prune), 0, (struct sockaddr *)&dest, sizeof(dest));
                            } else {
                                printf("         ✅ [MAJ-STABLE] Chemin maintenu optimal via le parent actuel %s (Coût: %d)\n", ip_p, mon_cout_actuel_vers_racine);
                            }
                        } else {
                            printf("         ❌ Chemin rejeté (Coût sous-optimal : %d > %d).\n", score_total, mon_cout_actuel_vers_racine);
                            pthread_mutex_unlock(&verrou_partage);
                        }
                    } else {
                        printf("         ℹ️ C'est ma propre adresse IP. Ignoré pour éviter les boucles.\n");
                    }
                }
                seg = strtok_r(NULL, ";", &ptr);
            }
            printf("   🔍 [ALGO-JACK] Fin de l'analyse du message JACK.\n");
        }

        /* 3.3 - DÉSENREGISTREMENT / ÉLAGAGE (PRUNE) */
        else if (strncmp(buf, "PRUNE", 5) == 0) {
            pthread_mutex_lock(&verrou_partage);
            if (strcmp(buf, "PRUNE-T-PMI") == 0 || strcmp(buf, "PRUNE-T-PMR") == 0) {
                for (int i = 0; i < nombre_terminaux_locaux; i++) {
                    if (strcmp(liste_terminaux_locaux[i], ip_exp) == 0) {
                        strcpy(liste_terminaux_locaux[i], liste_terminaux_locaux[--nombre_terminaux_locaux]);
                        printf("   🪓 [PRUNE] Terminal local %s retiré. Restants: %d\n", ip_exp, nombre_terminaux_locaux);
                        i--;
                    }
                }
            }
            else if (strcmp(buf, "PRUNE-PMI-PMI") == 0 || strcmp(buf, "PRUNE-PMI-PMR") == 0) {
                for (int i = 0; i < nombre_voisins_multicast; i++) {
                    if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) {
                        strcpy(liste_voisins_multicast[i], liste_voisins_multicast[--nombre_voisins_multicast]);
                        printf("   🪓 [PRUNE] Passerelle enfant %s retirée. Restantes: %d\n", ip_exp, nombre_voisins_multicast);
                        i--;
                    }
                }
            }

            // Règle d'Auto-élagage de l'arbre
            if (nombre_voisins_multicast == 0 && nombre_terminaux_locaux == 0 && !je_suis_la_racine) {
                printf("   🍃 [AUTO-PRUNE] Je n'ai plus aucun récepteur dépendant. Je m'élague de l'arbre.\n");
                struct sockaddr_in p = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                inet_pton(AF_INET, ip_de_mon_parent_actuel, &p.sin_addr);
                const char* msg = (strcmp(ip_de_mon_parent_actuel, ip_racine_pmr) == 0) ? "PRUNE-PMI-PMR" : "PRUNE-PMI-PMI";
                printf("   ➡️ Envoi de %s vers mon parent %s\n", msg, ip_de_mon_parent_actuel);
                sendto(ds, msg, strlen(msg), 0, (struct sockaddr *)&p, sizeof(p));
            }
            pthread_mutex_unlock(&verrou_partage);
        }
    }
}

/******************************************************************************
 * BLOC 4 : PLAN DE DONNÉES (DUPLICATION & ROUTAGE STRICT)
 *****************************************************************************/
void* thread_donnees(void* arg) {
    printf("[DATA] Thread de transmission de données actif sur le Port %d\n", PORT_DONNEES);
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

        /* FILTRAGE ANTI-BOUCLE ET ANTI-SPOOFING ULTRA-STRICT */
        /*if (!je_suis_la_racine &&
            strcmp(ip_source_paquet, ip_de_mon_parent_actuel) != 0 &&
            strcmp(ip_source_paquet, "10.0.3.1") != 0 &&
            strcmp(ip_source_paquet, "127.0.0.1") != 0) {
            printf("⚠️ [DATA-DROP] Paquet reçu de %s IGNORÉ. Raison: Ne provient pas du parent légitime (%s)\n",
                   ip_source_paquet, ip_de_mon_parent_actuel);
            pthread_mutex_unlock(&verrou_partage);
            continue;
        }*/


        // 1. Envoi vers les Terminaux Locaux branchés sur cette PMI
        for (int i = 0; i < nombre_terminaux_locaux; i++) {
            inet_pton(AF_INET, liste_terminaux_locaux[i], &d.sin_addr);
            sendto(sd, t, sz, 0, (struct sockaddr *)&d, sizeof(d));
        }

        // 2. Envoi vers les Passerelles Enfants Multicast (Descente d'arbre)
        for (int i = 0; i < nombre_voisins_multicast; i++) {
            if (strcmp(liste_voisins_multicast[i], ip_source_paquet) != 0) {
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
        printf("❌ Usage incorrect ! Exemple : ./gateway <IP_LOCALE> <CONFIG_JSON>\n");
        return 1;
    }

    // Capturer le signal système SIGUSR1 pour show_tree.sh
    signal(SIGUSR1, gestionnaire_signal_arbre);

    strcpy(mon_ip, argv[1]);
    charger_configuration(argv[2]);

    pthread_t c, d, s;
    pthread_create(&c, NULL, thread_controle, NULL);
    pthread_create(&d, NULL, thread_donnees, NULL);

    /*if (je_suis_la_racine) {
        pthread_create(&s, NULL, thread_source_flux, NULL);
    }*/

    printf("🟢 [SYSTEM-READY] Passerelle opérationnelle en mode : %s sur l'adresse %s\n\n",
           je_suis_la_racine ? "PMR (Racine)" : "PMI (Intermédiaire)", mon_ip);

    pthread_join(c, NULL);
    pthread_join(d, NULL);
    if (je_suis_la_racine) pthread_join(s, NULL);

    return 0;
}