#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

int main(int argc, char *argv[]) {
    if (argc < 2) { printf("Usage: %s <IP_PASSERELLE>\n", argv[0]); return 1; }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in serv = { .sin_family = AF_INET, .sin_port = htons(5000) };
    inet_pton(AF_INET, argv[1], &serv.sin_addr);

    // 1. Inscription
    sendto(sock, "JOIN-T-PMR", 10, 0, (struct sockaddr *)&serv, sizeof(serv));
    printf("[EMETTEUR] Inscription envoyée à %s. Envoi du flux...\n", argv[1]);

    // 2. Envoi des données (Port 6000)
    struct sockaddr_in dest = { .sin_family = AF_INET, .sin_port = htons(6000) };
    inet_pton(AF_INET, argv[1], &dest.sin_addr);

    int cpt = 0;
    char msg[128];
    while (1) {
        sprintf(msg, "PAQUET_MULTICAST_%d", cpt++);
        sendto(sock, msg, strlen(msg), 0, (struct sockaddr *)&dest, sizeof(dest));
        printf("[EMETTEUR] Envoi : %s\n", msg);
        sleep(2);
    }
    return 0;
}