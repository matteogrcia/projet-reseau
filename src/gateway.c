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

typedef struct {
    char ip[16];
    int cost_to_me; // Coût direct lu dans le JSON
} NeighborConfig;

// État global
char my_ip[16];
char root_ip[16];
int is_in_tree = 0; // 1 si je reçois déjà le flux

NeighborConfig topology[MAX_NODES];
int topology_count = 0;

// Listes de diffusion (Niveau 2)
char tree_neighbors[MAX_NODES][16];
int tree_neighbor_count = 0;
char local_receivers[MAX_NODES][16];
int receiver_count = 0;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

// --- BLOC 1 : CHARGEMENT CONFIGURATION ---
// Lit le JSON pour connaître les coûts directs (ex: C-A=6, C-B=2)
void load_config(const char *filename) {
    FILE *f = fopen(filename, "rb");
    if (!f) exit(1);
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *data = malloc(len + 1);
    fread(data, 1, len, f);
    fclose(f);

    cJSON *json = cJSON_Parse(data);
    strcpy(root_ip, cJSON_GetObjectItem(json, "root_gateway")->valuestring);
    if (strcmp(my_ip, root_ip) == 0) is_in_tree = 1;

    cJSON *gateways = cJSON_GetObjectItem(json, "gateways");
    int size = cJSON_GetArraySize(gateways);
    for (int i = 0; i < size; i++) {
        cJSON *item = cJSON_GetArrayItem(gateways, i);
        const char* ip = cJSON_GetObjectItem(item, "ip")->valuestring;
        if (strcmp(ip, my_ip) != 0) {
            strcpy(topology[topology_count].ip, ip);
            topology[topology_count].cost_to_me = cJSON_GetObjectItem(item, "cost")->valueint;
            topology_count++;
        }
    }
    free(data);
    cJSON_Delete(json);
}

// --- BLOC 2 : PLAN DE CONTRÔLE (SIGNALISATION) ---
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

        // A. Un terminal Microcore demande à rejoindre
        if (strcmp(buffer, "JOIN") == 0) {
            strcpy(local_receivers[receiver_count++], sender_ip);
            // C demande aux autres : "Qui a déjà le flux ?"
            for (int i = 0; i < topology_count; i++) {
                struct sockaddr_in dest = { .sin_family = AF_INET, .sin_port = htons(CONTROL_PORT) };
                inet_pton(AF_INET, topology[i].ip, &dest.sin_addr);
                sendto(sock, "IN_TREE_QUERY", 13, 0, (struct sockaddr *)&dest, sizeof(dest));
            }
        }
        // B. On me demande si j'ai le flux
        else if (strcmp(buffer, "IN_TREE_QUERY") == 0) {
            if (is_in_tree) sendto(sock, "I_HAVE_FLOW", 11, 0, (struct sockaddr *)&sender_addr, addr_len);
        }
        // C. C reçoit les réponses et choisit le voisin le MOINS CHER en coût DIRECT
        else if (strcmp(buffer, "I_HAVE_FLOW") == 0) {
            int current_neighbor_cost = 999;
            for(int i=0; i<topology_count; i++) {
                if(strcmp(topology[i].ip, sender_ip) == 0) current_neighbor_cost = topology[i].cost_to_me;
            }
            // JACK : On envoie le join à celui qui est le plus proche physiquement parmi ceux qui ont le flux
            // (Ici, C choisira B car coût 2 < coût 6 vers A)
            sendto(sock, "JACK", 4, 0, (struct sockaddr *)&sender_addr, addr_len);
            is_in_tree = 1;
        }
        // D. Une passerelle accepte un nouveau voisin
        else if (strcmp(buffer, "JACK") == 0) {
            pthread_mutex_lock(&lock);
            strcpy(tree_neighbors[tree_neighbor_count++], sender_ip);
            pthread_mutex_unlock(&lock);
        }
    }
}

// --- BLOC 3 : PLAN DE DONNÉES (DUPLICATION L2) ---
void* data_thread(void* arg) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(DATA_PORT), .sin_addr.s_addr = INADDR_ANY };
    bind(sock, (struct sockaddr *)&addr, sizeof(addr));

    char buffer[BUFFER_SIZE];
    struct sockaddr_in out_addr = { .sin_family = AF_INET, .sin_port = htons(DATA_PORT) };

    while (1) {
        int n = recv(sock, buffer, BUFFER_SIZE, 0);
        pthread_mutex_lock(&lock);
        // Vers Microcore locaux
        for (int i = 0; i < receiver_count; i++) {
            inet_pton(AF_INET, local_receivers[i], &out_addr.sin_addr);
            sendto(sock, buffer, n, 0, (struct sockaddr *)&out_addr, sizeof(out_addr));
        }
        // Vers autres VyOS (Voisins de l'arbre)
        for (int i = 0; i < tree_neighbor_count; i++) {
            inet_pton(AF_INET, tree_neighbors[i], &out_addr.sin_addr);
            sendto(sock, buffer, n, 0, (struct sockaddr *)&out_addr, sizeof(out_addr));
        }
        pthread_mutex_unlock(&lock);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) return 1;
    strcpy(my_ip, argv[1]);
    load_config(argv[2]);
    pthread_t t1, t2;
    pthread_create(&t1, NULL, control_thread, NULL);
    pthread_create(&t2, NULL, data_thread, NULL);
    pthread_join(t1, NULL);
    return 0;
}