#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <cjson/cJSON.h>

#define CONTROL_PORT 5000
#define DATA_PORT 6000
#define BUFFER_SIZE 2048
#define MAX_NODES 10

// Structure pour stocker la connaissance locale (issue du JSON)
typedef struct {
    char id[10];
    char ip[16];
    int cost_to_neighbor; // Coût de MOI vers ce voisin (lu dans le JSON)
} NeighborConfig;

// État global de la passerelle
char my_ip[16];
char root_ip[16];
int is_root = 0;
int my_dist_to_root = 999; // Infini au départ, 0 si PR

NeighborConfig topology[MAX_NODES];
int topology_count = 0;

// Listes de l'arbre de niveau 2
char tree_members[MAX_NODES][16]; // IPs des passerelles à qui je dois renvoyer le flux
int tree_member_count = 0;
char local_receivers[MAX_NODES][16]; // IPs des terminaux Microcore connectés ici
int receiver_count = 0;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

// --- CHARGEMENT DU JSON ---
void load_config(const char *filename) {
    FILE *f = fopen(filename, "rb");
    if (!f) { perror("Erreur fichier JSON"); exit(1); }
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(len + 1);
    fread(data, 1, len, f);
    fclose(f);

    cJSON *json = cJSON_Parse(data);
    strcpy(root_ip, cJSON_GetObjectItem(json, "root_gateway")->valuestring);
    if (strcmp(my_ip, root_ip) == 0) {
        is_root = 1;
        my_dist_to_root = 0;
    }

    cJSON *gateways = cJSON_GetObjectItem(json, "gateways");
    int size = cJSON_GetArraySize(gateways);
    for (int i = 0; i < size; i++) {
        cJSON *item = cJSON_GetArrayItem(gateways, i);
        const char* ip = cJSON_GetObjectItem(item, "ip")->valuestring;
        if (strcmp(ip, my_ip) != 0) {
            strcpy(topology[topology_count].ip, ip);
            topology[topology_count].cost_to_neighbor = cJSON_GetObjectItem(item, "cost")->valueint;
            topology_count++;
        }
    }
    free(data);
    cJSON_Delete(json);
}

// --- LOGIQUE DE DUPLICATION (NIVEAU 2) ---
void add_to_tree(char *ip) {
    pthread_mutex_lock(&lock);
    for (int i = 0; i < tree_member_count; i++) {
        if (strcmp(tree_members[i], ip) == 0) { pthread_mutex_unlock(&lock); return; }
    }
    strcpy(tree_members[tree_member_count++], ip);
    printf("[ARBRE] %s ajouté à la liste de diffusion\n", ip);
    pthread_mutex_unlock(&lock);
}

// --- THREAD DE CONTRÔLE (SIGNALISATION PORT 5000) ---
void* control_thread(void* arg) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(CONTROL_PORT), .sin_addr.s_addr = INADDR_ANY };
    bind(sock, (struct sockaddr *)&addr, sizeof(addr));

    char buffer[BUFFER_SIZE];
    struct sockaddr_in sender_addr;
    socklen_t addr_len = sizeof(sender_addr);

    while (1) {
        int n = recvfrom(sock, buffer, BUFFER_SIZE, 0, (struct sockaddr *)&sender_addr, &addr_len);
        buffer[n] = '\0';
        char *sender_ip = inet_ntoa(sender_addr.sin_addr);

        // 1. Un Microcore fait un JOIN
        if (strcmp(buffer, "JOIN") == 0) {
            printf("[CTRL] JOIN reçu du terminal %s\n", sender_ip);
            strcpy(local_receivers[receiver_count++], sender_ip);

            // Lancer le calcul du meilleur parent (JACK)
            // On interroge les autres passerelles pour connaître leur coût
            for (int i = 0; i < topology_count; i++) {
                struct sockaddr_in dest = { .sin_family = AF_INET, .sin_port = htons(CONTROL_PORT) };
                inet_pton(AF_INET, topology[i].ip, &dest.sin_addr);
                sendto(sock, "GET_COST", 8, 0, (struct sockaddr *)&dest, sizeof(dest));
            }
        }
        // 2. Quelqu'un demande mon coût vers la racine
        else if (strcmp(buffer, "GET_COST") == 0) {
            char reply[32];
            sprintf(reply, "MY_COST %d", my_dist_to_root);
            sendto(sock, reply, strlen(reply), 0, (struct sockaddr *)&sender_addr, addr_len);
        }
        // 3. Réception d'une réponse de coût pour calcul JACK
        else if (strncmp(buffer, "MY_COST", 7) == 0) {
            int remote_cost = atoi(buffer + 8);
            int local_link_cost = 0;
            for(int i=0; i<topology_count; i++) if(strcmp(topology[i].ip, sender_ip) == 0) local_link_cost = topology[i].cost_to_neighbor;

            int total_path = remote_cost + local_link_cost;
            printf("[JACK] Chemin via %s : coût total %d\n", sender_ip, total_path);

            if (total_path < 100) { // Condition simplifiée pour l'exemple : si chemin valide
                 sendto(sock, "JACK", 4, 0, (struct sockaddr *)&sender_addr, addr_len);
            }
        }
        // 4. Une passerelle m'a choisi comme parent
        else if (strcmp(buffer, "JACK") == 0) {
            add_to_tree(sender_ip);
        }
    }
}

// --- THREAD DE DONNÉES (DUPLICATION PORT 6000) ---
void* data_thread(void* arg) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(DATA_PORT), .sin_addr.s_addr = INADDR_ANY };
    bind(sock, (struct sockaddr *)&addr, sizeof(addr));

    char buffer[BUFFER_SIZE];
    struct sockaddr_in out_addr = { .sin_family = AF_INET, .sin_port = htons(DATA_PORT) };

    while (1) {
        int n = recv(sock, buffer, BUFFER_SIZE, 0);

        pthread_mutex_lock(&lock);
        // Dupliquer vers les Microcore locaux
        for (int i = 0; i < receiver_count; i++) {
            inet_pton(AF_INET, local_receivers[i], &out_addr.sin_addr);
            sendto(sock, buffer, n, 0, (struct sockaddr *)&out_addr, sizeof(out_addr));
        }
        // Dupliquer vers les passerelles voisines dans l'arbre
        for (int i = 0; i < tree_member_count; i++) {
            inet_pton(AF_INET, tree_members[i], &out_addr.sin_addr);
            sendto(sock, buffer, n, 0, (struct sockaddr *)&out_addr, sizeof(out_addr));
        }
        pthread_mutex_unlock(&lock);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) { printf("Usage: %s <Mon_IP> <config.json>\n", argv[0]); return 1; }
    strcpy(my_ip, argv[1]);
    load_config(argv[2]);

    pthread_t t1, t2;
    pthread_create(&t1, NULL, control_thread, NULL);
    pthread_create(&t2, NULL, data_thread, NULL);

    pthread_join(t1, NULL);
    pthread_join(t2, NULL);
    return 0;
}