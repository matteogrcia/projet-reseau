#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <signal.h> // REQUIS pour le handler

// Variables globales pour que le handler de signal puisse y accéder
int sock_controle;
struct sockaddr_in serv_controle;

// Fonction de capture du Ctrl+C (SIGINT)
void declencher_prune_on_exit(int signum) {
    printf("\n[Ctrl+C détecté] Envoi de la demande de désinscription (PRUNE)...\n");

    // On envoie le message exact attendu par le Bloc 3.3 de ta passerelle
    const char *msg_prune = "PRUNE-T-PMI";

    // Envoi à la passerelle (Port 5000)
    sendto(sock_controle, msg_prune, strlen(msg_prune), 0,
           (struct sockaddr *)&serv_controle, sizeof(serv_controle));

    printf("[CLIENT] Message de PRUNE envoyé avec succès. Fermeture.\n");

    close(sock_controle);
    exit(0);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <IP_PASSERELLE>\n", argv[0]);
        return 1;
    }

    // 1. Initialisation du socket de contrôle (Port 5000)
    sock_controle = socket(AF_INET, SOCK_DGRAM, 0);
    serv_controle.sin_family = AF_INET;
    serv_controle.sin_port = htons(5000);
    inet_pton(AF_INET, argv[1], &serv_controle.sin_addr);

    // INTERCEPTION DU CTRL+C
    // Dès que tu feras Ctrl+C, Linux exécutera la fonction 'declencher_prune_on_exit'
    signal(SIGINT, declencher_prune_on_exit);

    // Inscription (On utilise la taille réelle de la chaîne : strlen("JOIN-T-PMI") = 10)
    sendto(sock_controle, "JOIN-T-PMI", 10, 0, (struct sockaddr *)&serv_controle, sizeof(serv_controle));
    printf("[CLIENT] Inscription envoyée à %s. Écoute du flux...\n", argv[1]);

    // 2. Écoute des données (Port 6000)
    int sock_data = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in addr_data = { .sin_family = AF_INET, .sin_port = htons(6000), .sin_addr.s_addr = INADDR_ANY };
    bind(sock_data, (struct sockaddr *)&addr_data, sizeof(addr_data));

    char tampon[2048];
    while(1) {
        int n = recv(sock_data, tampon, 2048, 0);
        if (n <= 0) continue;
        tampon[n] = '\0';
        printf("[DATA RECEIVE] : %s\n", tampon);
    }

    close(sock_data);
    return 0;
}