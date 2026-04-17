1. Le Mécanisme de JOIN (L'Expansion de l'Arbre)

Le processus de JOIN est une réaction en chaîne qui part de l'utilisateur final pour remonter jusqu'à la source.
Étape A : La demande initiale (JOIN-T-PMI)

Lorsqu'un terminal récepteur (Microcore) veut recevoir le flux, il envoie un message JOIN-T-PMI à sa passerelle la plus proche.

    Action de la PMI : Elle enregistre l'adresse IP du terminal dans sa liste_terminaux_locaux. Désormais, elle sait qu'elle doit envoyer une copie de chaque paquet de données à cette adresse.

Étape B : La remontée vers la Racine (JOIN-PMI-PMR)

Si la passerelle ne reçoit pas encore le flux, elle doit le demander. Par défaut, elle s'adresse à la "mairie centrale" : la PMR.

    Action de la PMI : Elle consulte son config.json pour trouver l'IP de la racine et lui envoie JOIN-PMI-PMR.

Étape C : L'intégration et l'annonce (JACK)

Imaginons cette situation : PMI-A est déjà dans l'arbre. PMI-B (le nouveau) vient de se connecter à la Racine.
Cas 1 : Tu es le "Nouveau" (PMI-B)

Tu viens d'envoyer un JOIN-PMI-PMR. Tu ne connais que la Racine. La Racine te répond par un JACK contenant : [IP_Racine, Coût:0] ; [IP_PMI-A, Coût:5].

    Réception : Tu reçois le message. Ta variable mon_cout_actuel_vers_racine est par exemple à 15 (ton lien direct avec la Racine).

    La Boucle (L'Analyse) :

        Tour 1 (La Racine) : Ton code voit IP_Racine. La condition if(strcmp != mon_ip) est vraie. Tu calcules : Coût Racine (0) + Ton lien vers Racine (15) = 15. Ce n'est pas mieux que 15. On ne fait rien.

        Tour 2 (PMI-A) : Ton code voit IP_PMI-A. La condition if(strcmp != mon_ip) est vraie.

    Le Calcul : Tu regardes ton JSON. Il dit que le lien physique PMI-B <-> PMI-A coûte 2.

        Calcul : Distance Racine → A (5) + Distance A → Moi (2) = 7.

    La Décision : Ton code compare : 7 est-il inférieur à 15 ? OUI.

    L'Action : Tu envoies un JOIN à PMI-A et un PRUNE à la Racine. Tu t'es optimisé en montant dans l'arbre.

Cas 2 : Tu n'es PAS le nouveau (Tu es PMI-A, déjà là)

Tu es tranquillement dans l'arbre depuis 10 minutes. La Racine t'envoie un JACK car elle vient d'intégrer PMI-B. Le message contient : [IP_Racine, Coût:0] ; [IP_PMI-B, Coût:15].

    Réception : Tu reçois le même message. Ton mon_cout_actuel_vers_racine est à 5.

    La Boucle (L'Analyse) :

        Tour 1 (La Racine) : Tu es déjà lié à elle ou à un meilleur chemin. Rien ne change.

        Tour 2 (PMI-B, le nouveau) : Ton code voit l'IP du petit nouveau.

    Le Calcul : Tu regardes ton JSON. Il dit que le lien PMI-A <-> PMI-B coûte 2.

        Calcul : Distance Racine → B (15) + Distance B → Moi (2) = 17.

    La Décision : Ton code compare : 17 est-il inférieur à 5 ? NON.

    L'Action : Tu ne fais rien. Le nouveau ne t'apporte pas de raccourci. Tu restes sur ton chemin optimal.

Étape D : L'optimisation dynamique (JOIN-PMI-PMI)

Toutes les passerelles qui reçoivent le JACK font un calcul. Si l'une d'elles se rend compte que le nouveau venu est un "voisin physique" (via le JSON) et qu'il offre un chemin plus court vers la racine :

    Elle envoie un JOIN-PMI-PMI au nouveau venu pour changer de parent. L'arbre se réorganise tout seul pour être plus court.

2. Le Mécanisme de PRUNE (L'Élagage de l'Arbre)

Le PRUNE est le mécanisme inverse : il sert à "nettoyer" les branches de l'arbre qui ne servent plus à rien afin de libérer de la bande passante.
Étape A : Le désintérêt d'un client

Un utilisateur arrête de regarder le flux. Son terminal envoie un message PRUNE à sa passerelle.

    Action de la PMI : Elle retire l'IP du terminal de sa liste. Si elle a encore d'autres terminaux ou d'autres passerelles rattachées à elle, elle continue de recevoir le flux.

Étape B : La réaction en chaîne (L'auto-élagage)

C'est le point critique. Si, après avoir retiré le client, la passerelle constate que :

    Sa liste_terminaux_locaux est vide.

    Sa liste_voisins_multicast (les autres passerelles en dessous) est vide.

    Conclusion : Elle reçoit des données pour rien.

    Action : Elle envoie un message PRUNE-PMI-PMI à son propre parent.

Étape C : La propagation vers la source

Le parent reçoit le PRUNE, retire la passerelle de sa liste, et se pose la même question : "Est-ce que j'ai encore quelqu'un à nourrir ?". Si la réponse est non, il envoie un PRUNE à son propre parent, et ainsi de suite.

    Résultat : Une branche entière du réseau peut s'éteindre en quelques millisecondes si plus personne n'écoute à l'autre bout.