#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <time.h>

#define PORT_CONTROLE 5000
#define PORT_DONNEES 6000
#define TAILLE_TAMPON 2048

/* --- VARIABLES GLOBALES --- */
int sock_data;
unsigned long compteur_sequence_recue = 0;

/* --- UTILITAIRE D'HORODATAGE --- */
void obtenir_heure_hm(char *tampon, size_t taille) {
    time_t maintenant = time(NULL);
    struct tm *temps_local = localtime(&maintenant);
    strftime(tampon, taille, "%H:%M", temps_local);
}

/******************************************************************************
 * THREAD : RÉCEPTION EN CONTINU DES PAQUETS DE DONNÉES
 *****************************************************************************/
void* thread_reception_donnees(void* arg) {
    char tampon[TAILLE_TAMPON];
    char hm[10];

    while (1) {
        int n = recv(sock_data, tampon, TAILLE_TAMPON - 1, 0);
        if (n <= 0) continue;
        tampon[n] = '\0';

        compteur_sequence_recue++;
        obtenir_heure_hm(hm, sizeof(hm));

        printf("\n*************************************************\n");
        printf("%s DATA %lu\n", hm, compteur_sequence_recue);
        printf("*************************************************\n");
        printf("[DATA RECEIVE] Contenu : %s (%d octets)\n\n", tampon, n);
        printf("Action (j/p/q) : ");
        fflush(stdout);
    }
    return NULL;
}

/******************************************************************************
 * POINT D'ENTRÉE PRINCIPAL (PLAN DE CONTRÔLE INTERACTIF)
 *****************************************************************************/
int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Erreur : Usage incorrect ! Exemple : %s <IP_PASSERELLE>\n", argv[0]);
        return 1;
    }

    // 1. Initialisation du socket de contrôle (Port 5000)
    int sock_controle = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in serv_controle = { .sin_family = AF_INET, .sin_port = htons(PORT_CONTROLE) };
    inet_pton(AF_INET, argv[1], &serv_controle.sin_addr);

    // 2. Initialisation et attachement du socket de données (Port 6000)
    sock_data = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in addr_data = { .sin_family = AF_INET, .sin_port = htons(PORT_DONNEES), .sin_addr.s_addr = INADDR_ANY };
    if (bind(sock_data, (const struct sockaddr *)&addr_data, sizeof(addr_data)) < 0) {
        perror("Erreur bind data");
        exit(1);
    }

    // 3. Lancement du thread de réception des données en tâche de fond
    pthread_t t_data;
    pthread_create(&t_data, NULL, thread_reception_donnees, NULL);

    char tampon_saisie[10];
    char hm[10];

    obtenir_heure_hm(hm, sizeof(hm));
    printf("*************************************************\n");
    printf("%s INIT --\n", hm);
    printf("*************************************************\n");
    printf("Terminal recepteur connecte a : %s\n", argv[1]);
    printf("Menu : [j]=Join, [p]=Prune, [q]=Quitter\n");
    printf("*************************************************\n\n");

    while (1) {
        printf("Action (j/p/q) : ");
        fflush(stdout);

        if (fgets(tampon_saisie, sizeof(tampon_saisie), stdin) == NULL) continue;
        char commande = tampon_saisie[0];

        switch (commande) {
            case 'j':
            case 'J':
                obtenir_heure_hm(hm, sizeof(hm));
                sendto(sock_controle, "JOIN-T-PMI", 10, 0, (struct sockaddr *)&serv_controle, sizeof(serv_controle));
                printf("\n*************************************************\n");
                printf("%s JOIN-T-PMI --\n", hm);
                printf("*************************************************\n");
                printf("Demande d'inscription emise vers %s\n\n", argv[1]);
                break;

            case 'p':
            case 'P':
                obtenir_heure_hm(hm, sizeof(hm));
                sendto(sock_controle, "PRUNE-T-PMI", 11, 0, (struct sockaddr *)&serv_controle, sizeof(serv_controle));
                printf("\n*************************************************\n");
                printf("%s PRUNE-T-PMI --\n", hm);
                printf("*************************************************\n");
                printf("Demande d'elagage emise vers %s\n\n", argv[1]);
                break;

            case 'q':
            case 'Q':
                obtenir_heure_hm(hm, sizeof(hm));
                printf("\n*************************************************\n");
                printf("%s EXIT --\n", hm);
                printf("*************************************************\n");
                printf("Fermeture du terminal client.\n");
                pthread_cancel(t_data);
                close(sock_controle);
                close(sock_data);
                return 0;

            default:
                printf("Commande inconnue. Utilisez uniquement j, p ou q.\n\n");
                break;
        }
    }

    return 0;
}