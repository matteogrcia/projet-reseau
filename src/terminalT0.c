#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <time.h>

/* --- UTILITAIRE D'HORODATAGE --- */
void obtenir_heure_hm(char *tampon, size_t taille) {
    time_t maintenant = time(NULL);
    struct tm *temps_local = localtime(&maintenant);
    strftime(tampon, taille, "%H:%M", temps_local);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("❌ Usage incorrect ! Exemple : %s <IP_PASSERELLE>\n", argv[0]);
        return 1;
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    // Configuration de l'adresse de la passerelle (Port 5000 pour le Contrôle, Port 6000 pour la Data)
    struct sockaddr_in serv_controle = { .sin_family = AF_INET, .sin_port = htons(5000) };
    struct sockaddr_in serv_donnees  = { .sin_family = AF_INET, .sin_port = htons(6000) };
    inet_pton(AF_INET, argv[1], &serv_controle.sin_addr);
    inet_pton(AF_INET, argv[1], &serv_donnees.sin_addr);

    int compteur_sequence = 0;
    char tampon_saisie[10];
    char hm[10];

    printf("=========================================================\n");
    printf(" 🟢 SOURCE MULTICAST T0 EN LIGNE (Cible : %s)\n", argv[1]);
    printf("=========================================================\n");
    printf(" [j] + Entrée : Envoyer une demande d'inscription (JOIN-T-PMR)\n");
    printf(" [p] + Entrée : Envoyer une demande de désinscription (PRUNE-T-PMR)\n");
    printf(" [d] + Entrée : Injecter un paquet de données (DATA)\n");
    printf(" [q] + Entrée : Quitter le programme\n");
    printf("=========================================================\n\n");

    while (1) {
        printf("👉 Action (j/p/d/q) : ");
        fflush(stdout);

        // Lecture sécurisée de la commande utilisateur
        if (fgets(tampon_saisie, sizeof(tampon_saisie), stdin) == NULL) continue;

        // On extrait le premier caractère tapé
        char commande = tampon_saisie[0];

        switch (commande) {
            case 'j':
            case 'J':
                obtenir_heure_hm(hm, sizeof(hm));
                sendto(sock, "JOIN-T-PMR", 10, 0, (struct sockaddr *)&serv_controle, sizeof(serv_controle));
                printf("[%s] [CONTROL] Envoi de l'inscription 'JOIN-T-PMR' à %s\n\n", hm, argv[1]);
                break;

            case 'p':
            case 'P':
                obtenir_heure_hm(hm, sizeof(hm));
                sendto(sock, "PRUNE-T-PMR", 11, 0, (struct sockaddr *)&serv_controle, sizeof(serv_controle));
                printf("[%s] [CONTROL] Envoi de l'élagage 'PRUNE-T-PMR' à %s\n\n", hm, argv[1]);
                break;

            case 'd':
            case 'D':
                compteur_sequence++;
                obtenir_heure_hm(hm, sizeof(hm));

                char msg[128];
                sprintf(msg, "PAQUET_MULTICAST_%d", compteur_sequence);

                sendto(sock, msg, strlen(msg), 0, (struct sockaddr *)&serv_donnees, sizeof(serv_donnees));

                // Log calqué sur le formalisme demandé
                printf("*************************************************\n");
                printf("%s          DATA                                 %d\n", hm, compteur_sequence);
                printf("*************************************************\n");
                printf("🚀 Paquet injecté vers la racine : \"%s\"\n\n", msg);
                break;

            case 'q':
            case 'Q':
                printf("👋 Fermeture de la source T0.\n");
                close(sock);
                return 0;

            default:
                printf("⚠️ Commande inconnue. Utilisez uniquement j, p, d ou q.\n\n");
                break;
        }
    }

    close(sock);
    return 0;
}