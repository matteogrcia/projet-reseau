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

/******************************************************************************
 * BLOC 1 : CONFIGURATION
 *****************************************************************************/
void charger_configuration(const char *nom_fichier) {
    FILE *fichier = fopen(nom_fichier, "rb");
    if (!fichier) { perror("Erreur JSON"); exit(1); }

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
    int taille = cJSON_GetArraySize(passerelles);
    for (int i = 0; i < taille; i++) {
        cJSON *item = cJSON_GetArrayItem(passerelles, i);
        const char* ip = cJSON_GetObjectItem(item, "ip")->valuestring;
        if (strcmp(ip, mon_ip) != 0) {
            strcpy(topologie_reseau[nombre_voisins_physiques].adresse_ip, ip);
            cJSON *cost_item = cJSON_GetObjectItem(item, "cost");
            topologie_reseau[nombre_voisins_physiques].cout_direct = cost_item ? cost_item->valueint : 1;
            nombre_voisins_physiques++;
        }
    }
    strcpy(ip_de_mon_parent_actuel, ip_racine_pmr);

    // CORRECTION PHYSIQUE : Si je ne suis pas la racine, je cherche mon coût physique direct vers la racine PMR
    if (!je_suis_la_racine) {
        for (int i = 0; i < nombre_voisins_physiques; i++) {
            if (strcmp(topologie_reseau[i].adresse_ip, ip_racine_pmr) == 0) {
                mon_cout_actuel_vers_racine = topologie_reseau[i].cout_direct;
                break;
            }
        }
    }

    free(donnees_json);
    cJSON_Delete(objet_json);
}

/******************************************************************************
 * BLOC 2 : SOURCE DE FLUX (UNIQUEMENT PMR)
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
 * BLOC 3 : PLAN DE CONTRÔLE (SIGNALISATION)
 *****************************************************************************/
void* thread_controle(void* arg) {
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

        /* 3.1 - JOINS */
        if (strcmp(buf, "JOIN-T-PMI") == 0) {
            pthread_mutex_lock(&verrou_partage);
            int deja_present = 0;
            for (int i = 0; i < nombre_terminaux_locaux; i++) {
                if (strcmp(liste_terminaux_locaux[i], ip_exp) == 0) {
                    deja_present = 1;
                    break;
                }
            }
            if (!deja_present) {
                strcpy(liste_terminaux_locaux[nombre_terminaux_locaux++], ip_exp);
                printf("[JOIN] Terminal local ajouté : %s (Total: %d)\n", ip_exp, nombre_terminaux_locaux);
            }
            pthread_mutex_unlock(&verrou_partage);

            struct sockaddr_in r = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
            inet_pton(AF_INET, ip_racine_pmr, &r.sin_addr);
            sendto(ds, "JOIN-PMI-PMR", 12, 0, (struct sockaddr *)&r, sizeof(r));
        }
        else if (strcmp(buf, "JOIN-PMI-PMR") == 0 && je_suis_la_racine) {
            pthread_mutex_lock(&verrou_partage);

            int deja_present = 0;
            int index_voisin = -1;
            for (int i = 0; i < nombre_voisins_multicast; i++) {
                if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) {
                    deja_present = 1;
                    index_voisin = i;
                    break;
                }
            }
            if (!deja_present) {
                strcpy(liste_voisins_multicast[nombre_voisins_multicast], ip_exp);

                // CORRECTION PHYSIQUE : Chercher le vrai coût physique depuis la topologie chargée
                int cout_physique_associe = 1; // Valeur de secours si non trouvé
                for (int j = 0; j < nombre_voisins_physiques; j++) {
                    if (strcmp(topologie_reseau[j].adresse_ip, ip_exp) == 0) {
                        cout_physique_associe = topologie_reseau[j].cout_direct;
                        break;
                    }
                }
                couts_voisins_multicast[nombre_voisins_multicast++] = cout_physique_associe;
                printf("[JOIN-PMR] Passerelle enfant ajoutée : %s avec coût physique : %d\n", ip_exp, cout_physique_associe);
            }

            char jack[TAILLE_TAMPON];
            sprintf(jack, "JACK|%s,0;", mon_ip);
            for(int i=0; i < nombre_voisins_multicast; i++)
                sprintf(jack + strlen(jack), "%s,%d;", liste_voisins_multicast[i], couts_voisins_multicast[i]);
            pthread_mutex_unlock(&verrou_partage);

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
                if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) {
                    deja_present = 1;
                    break;
                }
            }
            if (!deja_present) {
                strcpy(liste_voisins_multicast[nombre_voisins_multicast++], ip_exp);
                printf("[JOIN-PMI] Passerelle enfant ajoutée : %s\n", ip_exp);
            }
            pthread_mutex_unlock(&verrou_partage);
        }

        /* 3.2 - OPTIMISATION JACK */
        else if (strncmp(buf, "JACK", 4) == 0) {
            char copie[TAILLE_TAMPON];
            strcpy(copie, buf + 5);
            char *seg, *ptr;
            seg = strtok_r(copie, ";", &ptr);
            while (seg != NULL) {
                char ip_p[16];
                int d_r;
                sscanf(seg, "%[^,],%d", ip_p, &d_r);
                if (strcmp(ip_p, mon_ip) != 0) {
                    int c_p = 999;
                    for(int i=0; i<nombre_voisins_physiques; i++)
                        if(strcmp(topologie_reseau[i].adresse_ip, ip_p) == 0) c_p = topologie_reseau[i].cout_direct;

                    pthread_mutex_lock(&verrou_partage);
                    if ((d_r + c_p) < mon_cout_actuel_vers_racine) {
                        char old[16];
                        strcpy(old, ip_de_mon_parent_actuel);
                        strcpy(ip_de_mon_parent_actuel, ip_p);
                        mon_cout_actuel_vers_racine = d_r + c_p;
                        pthread_mutex_unlock(&verrou_partage);

                        printf("[ALGO-JACK] Raccourci physique détecté via %s ! (Nouveau coût cumulé: %d)\n", ip_p, mon_cout_actuel_vers_racine);

                        struct sockaddr_in dest = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                        inet_pton(AF_INET, ip_de_mon_parent_actuel, &dest.sin_addr);
                        sendto(ds, "JOIN-PMI-PMI", 12, 0, (struct sockaddr *)&dest, sizeof(dest));

                        inet_pton(AF_INET, old, &dest.sin_addr);
                        const char* m_prune = (strcmp(old, ip_racine_pmr) == 0) ? "PRUNE-PMI-PMR" : "PRUNE-PMI-PMI";
                        sendto(ds, m_prune, strlen(m_prune), 0, (struct sockaddr *)&dest, sizeof(dest));
                    } else pthread_mutex_unlock(&verrou_partage);
                }
                seg = strtok_r(NULL, ";", &ptr);
            }
        }

        /* 3.3 - DÉSENREGISTREMENT (PRUNE) */
        else if (strncmp(buf, "PRUNE", 5) == 0) {
            pthread_mutex_lock(&verrou_partage);
            if (strcmp(buf, "PRUNE-T-PMI") == 0 || strcmp(buf, "PRUNE-T-PMR") == 0) {
                for (int i = 0; i < nombre_terminaux_locaux; i++) {
                    if (strcmp(liste_terminaux_locaux[i], ip_exp) == 0) {
                        strcpy(liste_terminaux_locaux[i], liste_terminaux_locaux[--nombre_terminaux_locaux]);
                        i--;
                    }
                }
                printf("[PRUNE] Nettoyage terminal %s fini (Restants: %d)\n", ip_exp, nombre_terminaux_locaux);
            }
            else if (strcmp(buf, "PRUNE-PMI-PMI") == 0 || strcmp(buf, "PRUNE-PMI-PMR") == 0) {
                for (int i = 0; i < nombre_voisins_multicast; i++) {
                    if (strcmp(liste_voisins_multicast[i], ip_exp) == 0) {
                        strcpy(liste_voisins_multicast[i], liste_voisins_multicast[--nombre_voisins_multicast]);
                        i--;
                    }
                }
                printf("[PRUNE] Nettoyage passerelle %s fini (Restantes: %d)\n", ip_exp, nombre_voisins_multicast);
            }

            if (nombre_voisins_multicast == 0 && nombre_terminaux_locaux == 0 && !je_suis_la_racine) {
                struct sockaddr_in p = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
                inet_pton(AF_INET, ip_de_mon_parent_actuel, &p.sin_addr);
                const char* msg = (strcmp(ip_de_mon_parent_actuel, ip_racine_pmr) == 0) ? "PRUNE-PMI-PMR" : "PRUNE-PMI-PMI";
                sendto(ds, msg, strlen(msg), 0, (struct sockaddr *)&p, sizeof(p));
                printf("[PRUNE] Auto-élagage vers le parent %s\n", ip_de_mon_parent_actuel);
            }
            pthread_mutex_unlock(&verrou_partage);
        }
    }
}

/******************************************************************************
 * BLOC 4 : PLAN DE DONNÉES (DUPLICATION)
 *****************************************************************************/
void* thread_donnees(void* arg) {
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
        if (!je_suis_la_racine &&
            strcmp(ip_source_paquet, ip_de_mon_parent_actuel) != 0 &&
            strcmp(ip_source_paquet, "127.0.0.1") != 0) {
            pthread_mutex_unlock(&verrou_partage);
            continue;
        }

        for (int i = 0; i < nombre_terminaux_locaux; i++) {
            inet_pton(AF_INET, liste_terminaux_locaux[i], &d.sin_addr);
            sendto(sd, t, sz, 0, (struct sockaddr *)&d, sizeof(d));
        }

        for (int i = 0; i < nombre_voisins_multicast; i++) {
            if (strcmp(liste_voisins_multicast[i], ip_source_paquet) != 0) {
                inet_pton(AF_INET, liste_voisins_multicast[i], &d.sin_addr);
                sendto(sd, t, sz, 0, (struct sockaddr *)&d, sizeof(d));
            }
        }
        pthread_mutex_unlock(&verrou_partage);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) return 1;
    strcpy(mon_ip, argv[1]);
    charger_configuration(argv[2]);
    pthread_t c, d, s;
    pthread_create(&c, NULL, thread_controle, NULL);
    pthread_create(&d, NULL, thread_donnees, NULL);

    //if (je_suis_la_racine) pthread_create(&s, NULL, thread_source_flux, NULL);

    printf("[SYSTEM] %s ON %s\n", je_suis_la_racine ? "PMR" : "PMI", mon_ip);
    pthread_join(c, NULL); pthread_join(d, NULL);
    return 0;
}