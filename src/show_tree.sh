#!/bin/bash

# Trouver les PIDs de tous les processus de passerelle lancés
PIDS=$(pgrep -f gateway)

if [ -z "$PIDS" ]; then
    echo "[-] Aucune passerelle applicative (gateway) n'est en cours d'exécution."
    exit 1
fi  

echo "================================================================="
echo "   ENVOI DU SIGNAL DE DIAGNOSTIC AUX PASSERELLES MULTICAST"
echo "================================================================="
echo "[+] Processus détectés (PIDs) : " $PIDS
echo "[+] Envoi du signal SIGUSR1 (kill -10)..."

# Envoyer le signal 10 (SIGUSR1) à toutes les passerelles trouvées
for pid in $PIDS; do
    kill -10 $pid 2>/dev/null
done

echo "[+] C'est fait ! Regardez les fenêtres de vos terminaux de passerelles."
echo "================================================================="