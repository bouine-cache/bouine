# Linus review — PR #731 `feat(cluster): coalesced origin shield for strong mode`

- **PR**: https://github.com/bouine-cache/bouine/pull/731
- **Branch**: `feat/cluster-origin-shield` (head `5ffe7f80`)
- **Date**: 2026-09-27
- **Mode**: deep (diff complet, fichiers en entier, appelants vérifiés, bug n°1 prouvé par test d'attaque sur le head de la PR ; `go test -race -short` vert, 1538 pass)

## Verdict

**Fix-before-merge.** L'idée est bonne et le latch unifié est le bon design.
Mais il y a un bug d'empoisonnement de cache prouvé, un panic sur nœud solo,
et la description de la PR se contredit trois fois dans le code.

## Findings (pire d'abord)

### 1. BLOCKER — Un HEAD coalescé empoisonne le cache de l'owner avec un objet sans corps

- **Où** : `internal/cache/origin_shield.go:95`, `origin_shield.go:207`
- La description de la PR affirme « HEAD requests are canonicalized to GET in
  the envelope ». **Le code ne fait aucune canonicalisation** —
  `Method: string(ctx.Method())` envoie HEAD tel quel. `FetchOrigin` accepte
  HEAD, fetch l'origine en HEAD (corps vide, par HTTP), puis
  `storeObjectLocal` stocke inconditionnellement un objet 200 sans corps sous
  la clé GET canonique.
- **Preuve** : test d'attaque sur le head de la PR — `FetchOrigin` avec une
  enveloppe HEAD contre une origine réaliste → objet stocké, `bodyLen=0,
  status=200`. Tout GET subséquent en peer-fetch depuis n'importe quel nœud
  reçoit un 200 vide jusqu'à expiration du TTL. `maybeBackfill` propage en
  plus l'objet vide sur le store du waiter. Aucun test ne couvre ce cas (le
  chemin `!isHEAD` de `streamMissBuffered` protège le path local, pas celui-ci).
- **Fix** : exactement ce que la description prétend avoir fait —
  canonicaliser HEAD→GET dans `originEnvelope` (ou au début de `FetchOrigin`).
  La suppression du corps côté waiter existe déjà dans `serveObject`.

### 2. bug — Nœud solo : panic garanti sur `/v1/peer/fetch`

- **Où** : `cmd/bouine/cmd/engine.go:807`, `internal/cluster/peerfetch.go:1219`
- `swapAdminHandler` s'exécute même sans cluster (`initCluster` retourne
  `nil, nil…` si `Listen.Cluster` est vide). `SetOwnerCheck(rs.clusterNode.IsLocal)`
  lie une méthode sur récepteur nil. `/v1/peer/fetch` est **exempté de
  token** (`internal/admin/server.go:760`) : un POST v3 coalescé sur le port
  admin d'un déploiement single-node → `handleCoalesce` → `c.Owner(key)` →
  déréférencement nil. Le recover du middleware admin le transforme en 500 +
  stack trace par requête — spam de log et violation de la règle
  « no panic outside main » (AGENTS.md §4). Ce panic n'existait pas avant
  cette PR.
- **Fix** : ne poser le check que si `rs.clusterNode != nil` (une porte
  existe déjà dans `handleCoalesce` pour ça).

### 3. Conception — Le comportement de base est derrière un flag opt-in qui ne gouverne pas ce qu'il nomme

- **Où** : `internal/config/config.go:379`, `cmd/bouine/cmd/builder.go:396`
- `peer_fetch_coalesce` défaut `false`. Décision de design : le shield est le
  **comportement de base du mode strong**, pas une fonctionnalité — le
  default doit être on (le flag peut rester comme porte de sortie
  `peer_fetch_coalesce: false` pour les déploiements prudents), la séquence
  de rollout gérant le risque de déploiement. Le knob
  `peer_fetch_backfill_probability` reste le seul tuning — c'est le cas
  « droits sur le keyspace » configurable, déjà correctement présenté
  (nil = 1.0, 0.0 = partition stricte).
- Pire : le flag est un mensonge d'interface. `routeFetchers` est peuplé
  inconditionnellement et `handleCoalesce` ne consulte jamais la config
  locale — un owner avec `peer_fetch_coalesce: false` coalesce quand même
  pour les waiters qui demandent. Le flag ne contrôle que le côté requester.
  Si un flag ne décrit pas le comportement du nœud sur lequel il est posé,
  ce n'est pas un flag.
- Note AGENTS.md §13 : les flags expérimentaux vivent sous `experimental:` ;
  actuellement ce flag est ni base-behavior ni expérimental. Le risque de
  déploiement se traite par la séquence de rollout, pas par un default off
  éternel.

### 4. bullshit — La description de la PR décrit du code qui n'existe pas (3 fois)

- (a) Le rôle `fallback` de `bouine_coalesced_fetch_total` — « fallback
  counts waiter-side waits that failed, e.g. owner down, which owner-side
  counters cannot see » : **aucun code n'incrémente `fallback`** ; les
  fallbacks waiter sont invisibles, exactement l'angle mort que la
  description prétend couvrir.
- (b) La canonicalisation HEAD→GET (finding 1).
- (c) « polls a converged ring instead of sleeping » :
  `test/integration/cluster_origin_shield_test.go:44` fait
  `time.Sleep(time.Second)` — qui viole aussi la règle no-sleeps
  d'AGENTS.md §8.
- Les descriptions de PR sont des contrats ; celle-ci décrit la PR qu'on
  aurait voulu relire.

### 5. taste — Le même 30 s écrit deux fois avec un aveu de dette

- **Où** : `internal/cache/origin_shield.go:25` et `internal/cluster/peerfetch.go:63`
- Le commentaire dit « the two constants must be changed together ».
  `pkg/api` est un kernel partagé importable par les deux couches — y poser
  la constante retire l'aveu d'impasse. Le commentaire actuel documente un
  piège au lieu de le retirer.

### 6. nit — Double écriture de réponse sur shed

- `runCoalescedFlight` écrit `ctx.Error(503)` **et** retourne l'erreur ;
  `handleCoalesce` réécrit par-dessus avec `ctx.Error(502)`. La sémantique
  503 + `Retry-After` soignée partout ailleurs (`writeShed503`) est écrasée.
  Un seul écrivain.

### 7. nit — Troncature silencieuse dans l'enveloppe

- `putLPString` (`internal/cluster/peerfetch.go:734`) tronque silencieusement
  au-delà de 64 KiB. Inatteignable aujourd'hui (cap URL 8 KiB), mais rejeter
  (`ok=false`) plutôt que tronquer évite qu'un futur relâchement de cap
  expédie une URI d'origine tronquée en production.

## Ce qui est bon, en une ligne

Le latch unifié avec le chemin foreground (commit 2) est le vrai design — un
seul singleflight par clé, c'est ça qui rend « 1 requête origine pour tout le
cluster » vrai — et la porte d'ownership (commit 3) ferme la double-fetch en
anneau convergent.

## Ordre de merge suggéré

1. Corriger 1 et 2 (bloquants, un soir chacun) — ajouter les tests qui
   manquent : HEAD coalescé, nœud solo + v3 coalescé.
2. Trancher le 3 selon le contrat de design (défaut on, ou suppression du
   flag ; le backfill reste le seul knob).
3. 4-5 avant merge (description de PR honnête + constante partagée).
4. 6-7 peuvent suivre en suivi.
