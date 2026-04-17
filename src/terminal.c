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
    sendto(sock, "JOIN-T-PMI", 10, 0, (struct sockaddr *)&serv, sizeof(serv));
    printf("[CLIENT] Inscription envoyée à %s. Écoute du flux...\n", argv[1]);

    // 2. Écoute des données (Port 6000)
    int sock_data = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in addr_data = { .sin_family = AF_INET, .sin_port = htons(6000), .sin_addr.s_addr = INADDR_ANY };
    bind(sock_data, (struct sockaddr *)&addr_data, sizeof(addr_data));

    char tampon[2048];
    while(1) {
        int n = recv(sock_data, tampon, 2048, 0);
        tampon[n] = '\0';
        printf("[DATA RECEIVE] : %s\n", tampon);
    }
    return 0;
}