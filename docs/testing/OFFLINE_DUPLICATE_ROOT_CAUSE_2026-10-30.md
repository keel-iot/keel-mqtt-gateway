# Doppio accodamento offline ordinario — 30 ottobre 2026

Causa riprodotta con immagine precedente al fix del resolver join
(`keel-mqtt-gateway:shared-optimization-20261030`).

Quando l'owner offline è l'edge che conserva la sessione Mochi disconnessa,
il ricevitore del forwarding esegue due percorsi per lo stesso messaggio:

1. `mqttServer.Publish` in `cmd/server/main.go` trova la subscription
   ordinaria del client locale disconnesso. Mochi crea e persiste l'inflight
   prima di verificare che la connessione sia chiusa; per le subscription
   ordinarie conserva questo stato per il successivo resume.
2. Il callback esegue anche `broker.DeliverOffline`, che accoda nuovamente
   il messaggio in Redis. Il suo dedup per publish ID non copre l'inflight
   creato dal primo percorso.

Prima del reconnect il test osserva lo stesso payload in due campi Redis:
packet ID 1, record Mochi con id "1" e origin "inline"; packet ID 2,
record offline Keel con id "<client>:2". Il reconnect sull'altro edge
legge entrambi e il client riceve packet ID 1 e 2 con lo stesso payload.
Non è la ritrasmissione dello stesso packet ID per un PUBACK perso.

Due client ID distinti, osservati con owner offline edge-1 e sessione
originaria edge-1, riproducono il doppio record e la doppia consegna.
Tre prove con owner offline edge-2 e sessione originaria edge-1 producono
un solo record e passano. La probabilità apparente del test dipende anche
dalla vista dei nodi durante il placement, non soltanto dalla riconnessione.

Il problema è indipendente dal nuovo retry di risoluzione dell'indirizzo.
Il percorso shared esclude già i membri locali disconnessi dalla selezione;
la subscription ordinaria resta invece nel percorso locale di Mochi.

La futura correzione deve assegnare un solo percorso di accodamento alle
sessioni offline nel cluster, conservando il comportamento offline di Mochi
in standalone e la consegna alle normali subscription connesse. Non va
risolta cancellando genericamente messaggi con payload uguale: due publish
legittimi possono avere lo stesso contenuto.

Log e dati sintetici:
`/home/fabio/keel-benchmark-lab/offline-duplicate-20261030`.

## Correzione

Keel filtra dalla consegna locale le subscription ordinarie dei client già
disconnessi quando sono configurati cluster e store offline distribuito.
Il dispatcher offline di Keel è quindi l'unico percorso che accoda quel
publish per quella sessione. I client connessi e inline restano selezionati.
Standalone e cluster senza store offline conservano la persistenza di Mochi.

Il fork invoca `OnSelectSubscribers` anche in presenza di sole subscription
ordinarie: prima quel hook era chiamato soltanto se esisteva almeno un gruppo
shared. Non vengono rimossi i dati delle sessioni, né deduplicati payload
uguali: due publish legittimi identici restano due messaggi.

Il test cluster `TestOfflineSession_OriginalEdgeOwnerDoesNotDuplicate` attende
la membership e forza un client ID con owner offline sull'edge originale.
Prima della riconnessione richiede esattamente un record Redis e poi una
sola consegna. Sull'immagine precedente fallisce trovando due record.
I test locali esercitano il vero publish Mochi senza filtri shared, con QoS
1/2, standalone/cluster e store presente/assente, oltre alla conservazione
dei destinatari connessi/inline.

## Esito della verifica

Il test deterministico sull'immagine precedente fallisce con due record
persistiti prima del reconnect. Sull'immagine corretta passa con un solo
record e una sola consegna. Sono passati tutti gli otto scenari cluster
selezionati: happy path multinodo, recupero offline sull'altro edge, owner
offline sull'edge originale, QoS 2 dopo perdita dell'edge, 20 membri shared
senza crescita offline, resume shared sullo stesso edge, selezione di un
solo membro shared offline e soppressione della coda offline con membri live.

Passati inoltre la suite generale e il race detector su broker/dataplane
e server del fork Mochi. I test ordinari offline di Mochi restano verdi.
Non è stata ripetuta la campagna completa dei benchmark per questa correzione.
Non si deduce da questi casi l'assenza di duplicati QoS 1 in ogni transizione
concorrente: QoS 1 mantiene le proprie garanzie at-least-once. La correzione
elimina il doppio accodamento riprodotto per la sessione già offline.
