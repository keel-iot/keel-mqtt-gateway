# Profilatura edge e Redis: QoS0, QoS1 e QoS2

La profilatura mostra due costi distinti: lavoro comune di routing/shared/gRPC e
operazioni Redis di persistenza QoS. Anche QoS0 scrive su Redis per contabilizzare
i byte del tenant. La build locale riduce la memoria residente rispetto alla
0.3.2, ma introduce protezioni dello schema inflight che aumentano il costo Redis
per QoS2. Non si deve dedurre un miglioramento generale di throughput dal solo
miglioramento della memoria.

## Metodo e limiti

Laboratorio isolato sul PC Ubuntu, generatore Go/Paho ARM64 sul Raspberry Pi 4.
Tre core, un Redis dedicato a ciascun core (primary/replicas governati da Raft,
non Redis Cluster), tre edge e PostgreSQL. Redis: AOF, appendfsync everysec,
backlog 128 MiB, quota 0,75 CPU/istanza. Edge: 1 CPU e GOMAXPROCS=1. Core: 0,25 CPU.
Nessuna modifica al codice dei broker durante le prove. Nessuna attivazione
del nuovo schema direzionale è stata richiesta: la build locale è misurata
nel percorso di compatibilità predefinito. Non è un confronto schema1/schema2. La build locale è
`keel-mqtt-gateway:cluster-test`, imageID
`sha256:f957f435352b978a66b42c462ec1d11a50a2a5e6f20fdc293a0b98f0700b0423`.
Release 0.3.2 ufficiale per il riferimento, con digest conservato nei manifest.

MQTT 3.1.1, clean_session=true, 256 byte, niente retained/TLS, credenziali da file,
ACL abilitate. Tutti i publisher entrano su edge 1; 12 consumer shared sono ripartiti
tra edge 2/3. Misura 60 secondi dopo 20 secondi di attesa connessa senza traffico.
I consumer usano lo stesso QoS dei publisher tranne i casi marcati `sub1`, che
simulano publisher QoS2 e consumer QoS1 come Redpanda Connect.
Questa topologia forza il percorso di inoltro e non misura la capacità aggregata
con ingressi bilanciati su tutti gli edge. Non ci sono Kafka/output connector.

CPU pprof 25 secondi dal secondo 5; allo stesso tempo profilo delta delle allocazioni
su tutti gli edge. Heap con GC prima della finestra, heap senza GC al secondo 38,
heap con GC dopo il carico. Il campionamento delle allocazioni è quello standard
Go: le quantità sono stime scalate, non contatori esatti di ogni malloc.
MiB/s allocati significa memoria creata e poi eventualmente liberata, non aumento
della RAM residente. Le percentuali cumulative si sovrappongono e non si sommano.
La profilatura può alterare CPU e latenza: non è una classifica performance.

Redis INFO prima/dopo sia con client connessi inattivi sia durante il traffico.
Le frequenze sono delta/tempo effettivo del server, circa 60 secondi. INFO e attività
periodiche rimangono nei contatori; il riferimento inattivo è conservato.
I comandi interni Lua sono contati da INFO commandstats: EVALSHA+INCRBY+EXPIRE
sono tre esecuzioni contabilizzate ma non tre roundtrip del client.
Non si sommano il tempo inclusivo di EVALSHA e i tempi dei comandi chiamati da Lua.

I risultati descrivono questo workload, non riproducono tutta la produzione GKE:
nessun retained, offline backlog o traffico eterogeneo degli 8.600 device. Il numero
di device da solo non determina il carico: servono msg/s, payload, QoS delle due
tratte, retained e fanout. Non sono prove di tolleranza ai guasti o disco pieno.

## Consegna e Redis

| Build / prova | QoS ricevuto | Messaggi ricevuti/attesi | Inviati nella finestra | Redis master comandi/s | Redis master CPU | Criterio frequenza |
|---|---|---:|---:|---:|---:|---|
| current-3200-qos0 | 0: 192000 | 192000/192000 | 192000 | 9992 | 0.119 | mantenuta |
| current-3200-qos1 | 1: 192000 | 192000/192000 | 192000 | 29011 | 0.371 | mantenuta |
| current-3200-qos2 | 2: 192000 | 192000/192000 | 191949 | 48165 | 0.533 | mantenuta |
| current-3200-qos2-sub1 | 1: 192000 | 192000/192000 | 192000 | 48043 | 0.554 | mantenuta |
| current-6400-qos0 | 0: 384000 | 384000/384000 | 383997 | 19993 | 0.201 | mantenuta |
| current-6400-qos2 | 2: 384000 | 384000/384000 | 312652 | 77212 | 0.637 | non mantenuta |
| release-0.3.2-3200-qos0 | 0: 192000 | 192000/192000 | 192000 | 10030 | 0.114 | mantenuta |
| release-0.3.2-3200-qos2 | 2: 192000 | 192000/192000 | 191998 | 22569 | 0.432 | mantenuta |
| release-0.3.2-3200-qos2-sub1 | 1: 192000 | 192000/192000 | 192000 | 22674 | 0.419 | mantenuta |

La prova locale a 6.400 msg/s QoS2 completa la consegna nel drain, ma non sostiene 6.400 msg/s
nella finestra: 312.652 invii e 310.717 consegne entro 60 secondi, contro 384.000 attesi;
ritardo di pianificazione p99 circa 13,4 secondi. Il flag `generator_limited` indica
violazione del criterio di invio, non prova che la CPU del Pi sia il collo di
bottiglia: il client attende il completamento QoS per ciascun invio. In quel punto
il Pi usa circa 1,65 core, l'edge d'ingresso circa 0,93/1 core, Redis master circa 0,65
core con throttling CPU (circa 2,1 s aggregati nella finestra). Questi dati sono
compatibili con backpressure del sistema e dei limiti impostati; non isolano
una capacità massima intrinseca del broker. I dati sono conservati senza
presentare il punto come frequenza sostenuta o attribuire il limite al solo Pi.

## CPU e allocazioni degli edge

| Prova | Edge | CPU campionata (core) | Allocazioni stimate MiB/s |
|---|---|---:|---:|
| current-3200-qos0 | edge-1 | 0.341 | 40.7 |
| current-3200-qos0 | edge-2 | 0.078 | 8.8 |
| current-3200-qos0 | edge-3 | 0.197 | 30.4 |
| current-3200-qos1 | edge-1 | 0.471 | 45.8 |
| current-3200-qos1 | edge-2 | 0.183 | 13.4 |
| current-3200-qos1 | edge-3 | 0.473 | 47.5 |
| current-3200-qos2 | edge-1 | 0.803 | 59.4 |
| current-3200-qos2 | edge-2 | 0.194 | 12.8 |
| current-3200-qos2 | edge-3 | 0.502 | 46.7 |
| current-3200-qos2-sub1 | edge-1 | 0.774 | 59.0 |
| current-3200-qos2-sub1 | edge-2 | 0.488 | 47.1 |
| current-3200-qos2-sub1 | edge-3 | 0.180 | 13.6 |
| current-6400-qos0 | edge-1 | 0.647 | 84.6 |
| current-6400-qos0 | edge-2 | 0.127 | 18.2 |
| current-6400-qos0 | edge-3 | 0.329 | 62.6 |
| current-6400-qos2 | edge-1 | 0.933 | 95.0 |
| current-6400-qos2 | edge-2 | 0.562 | 77.3 |
| current-6400-qos2 | edge-3 | 0.215 | 21.4 |
| release-0.3.2-3200-qos0 | edge-1 | 0.310 | 45.4 |
| release-0.3.2-3200-qos0 | edge-2 | 0.090 | 12.0 |
| release-0.3.2-3200-qos0 | edge-3 | 0.173 | 27.7 |
| release-0.3.2-3200-qos2 | edge-1 | 0.662 | 57.3 |
| release-0.3.2-3200-qos2 | edge-2 | 0.208 | 15.4 |
| release-0.3.2-3200-qos2 | edge-3 | 0.409 | 38.9 |
| release-0.3.2-3200-qos2-sub1 | edge-1 | 0.629 | 54.6 |
| release-0.3.2-3200-qos2-sub1 | edge-2 | 0.368 | 37.9 |
| release-0.3.2-3200-qos2-sub1 | edge-3 | 0.182 | 14.8 |

I due edge consumer non hanno necessariamente la stessa quota di messaggi:
le assegnazioni shared per topic non sono uniformi. Per confrontare i ricevitori
si usano anche distribuzione per subscriber e somma dei due edge, non il solo
edge 2 contro edge 3. I report conservano l'effettivo QoS ricevuto.

I profili principali della build locale a 3.200 msg/s mostrano:

- **Snapshot delle shared subscription e scelta del consumer.**
  `TopicsIndex.gatherSharedSubscriptions` è il maggior allocatore flat sull'edge
  d'ingresso: circa 123 MiB in 25 s con QoS0 e 162 MiB con QoS2. `SelectShared`,
  `MergeSharedSelected` e `publishSharedToSubscribers` compaiono anche sui ricevitori.
  Il fork ha già rimosso la copia intermedia `SharedSubscriptions.GetAll`, che è
  ancora visibile nei profili 0.3.2; rimane però churn consistente per messaggio.
- **gRPC per singolo inoltro.** Creazione stream unary, header HTTP2, contesti con
  deadline e serializzazione protobuf hanno peso rilevante. La metrica di inoltro
  nel caso QoS0 è coerente con circa una RPC riuscita per pubblicazione, non una
  RPC a tutti i consumer. Serve misurare prima di proporre batching/stream persistenti,
  che cambierebbero flush, ordinamento, backpressure e comportamento in errore.
- **Redis sugli edge.** In QoS2 a 3.200 msg/s il ramo `redis.Client.Process` contiene
  circa 32% dei campioni CPU dell'ingresso; a 6.400 sotto pressione circa 39%.
  È CPU inclusiva del client Redis, non tempo di attesa totale della rete né CPU
  del server Redis. Le chiamate agli hook QoS, letture schema e scritture sono visibili.
- **QoS2 e serializzazione.** `Inflight.SetInbound`, `encoding/json.Marshal` e
  `storage.Message.MarshalBinary` aggiungono allocazioni. Nel metodo locale
  `OnQosPublishResult` il messaggio viene serializzato prima del ciclo e nuovamente
  dopo aver scelto il campo/schema: una serializzazione iniziale ridondante da rimuovere.
- **Output disabilitati.** `forwardToOutputConnector` costruisce una richiesta,
  una mappa header e UUID stringa anche con zero connector. Nei profili dell'ingresso
  resta visibile con decine di MiB allocati in 25 s. Un ritorno anticipato può evitare
  la costruzione; non implica automaticamente disabilitare il contatore byte.

## Perché QoS0 usa Redis e cosa cambia in QoS2

`keelHook.OnPublish` chiama `withinDataVolumeLimit` per ogni QoS. Questa invoca
`forwarder.CheckAndRecordBytes` anche quando la quota è 0, ossia illimitata, e
quando non ci sono output connector. Lo script incrementa il contatore e aggiorna
la TTL ogni messaggio: EVALSHA→INCRBY+EXPIRE. Il costo è presente anche in 0.3.2.
Non sono inflight QoS0 salvati erroneamente. Il routing/session inventory produce
inoltre comandi periodici come HLEN, presenti anche nel riferimento inattivo.
Retained QoS0, non usato qui, avrebbe un altro percorso di persistenza Redis.

A 3.200 msg/s, nella build locale i contatori indicano approssimativamente:

| QoS pub→sub | Script byte/msg | HSET/msg | HDEL/msg | GET schema/msg | Altri script di persistenza/msg |
|---|---:|---:|---:|---:|---:|
| 0→0 | 1 | 0 | 0 | 0 | 0 |
| 1→1 | 1 | 1 | 1 | 3 | 1 |
| 2→2 | 1 | 2 | 2 | 6 | 2 |
| 2→1 | 1 | 2 | 2 | 6 | 2 |

GET comprende letture client e quelle interne agli script di fencing. Il dato
non equivale al numero di roundtrip. Gli hash/completamenti crescono con i flussi
inbound e outbound, non semplicemente con il conteggio dei publisher.
Nel caso publisher QoS2 → consumer QoS1 a 3.200 msg/s, la 0.3.2 misura
circa 22.700 comandi/s e 0,42 core Redis; la build locale circa 48.000 comandi/s
e 0,55 core. Entrambe consegnano 192.000/192.000 messaggi, effettivamente a QoS1,
senza duplicati o errori. HSET e HDEL restano circa due per messaggio:
abbassare il QoS del solo consumer non elimina questi salvataggi/completamenti.
Questo caso è più vicino alla tratta verso Redpanda Connect, pur senza riprodurre
l'intera configurazione e il traffico GKE.

## Confronto con 0.3.2 e priorità di intervento

Il riferimento 2→2 a 3.200 msg/s usa circa 22.600 comandi/s e 0,43 core Redis; il codice
locale circa 48.200 comandi/s e 0,53 core. L'edge d'ingresso passa da circa 0,66 a 0,80
core nei profili CPU. La nuova gestione dello schema consulta il marcatore prima
degli hook e dentro gli script atomici: protegge il cambio schema, ma aumenta il
lavoro sul percorso dei messaggi. Non è corretto promettere minore CPU QoS2 con
le sole modifiche di memoria. La sicurezza della migrazione non va rimossa.

Interventi suggeriti, non implementati in questa campagna:

1. **Evitare lavoro inutile nei connector e la doppia serializzazione inflight.**
   Cambi circoscritti, verificabili con test di persistenza e nuovi profili.
2. **Ridurre operazioni e roundtrip per lo schema inflight mantenendo il fencing.**
   Valutare consolidamento nella stessa operazione atomica e gestione esplicita
   del cambio schema. Non basta una cache TTL che possa usare campi sbagliati;
   vanno ripetuti i test rolling upgrade, rollback e collisione packet ID.
3. **Rivedere il contatore byte.** È una scrittura per messaggio anche senza quota;
   prima di eliminarla o aggregarla, chiarire se deve alimentare fatturazione,
   telemetria o limiti esatti e quali perdite di precisione sono accettabili.
   Cambiare la frequenza EXPIRE riduce scritture/AOF, ma va preservata la scadenza.
4. **Ridurre le copie per shared subscription.** Snapshot immutabili/versionati o
   strutture riutilizzabili richiedono attenzione a concorrenza e proprietà dei dati.
5. **Rivedere il data plane gRPC dopo i cambi circoscritti.** Batching/streaming è
   un intervento più ampio, da testare con consumer lenti, failover e timeout.

## Artefatti

`/home/fabio/keel-benchmark-lab/edge-profiling-20261029` contiene manifest,
configurazioni, ID immagini, binari corrispondenti ai profili, generatori con sorgenti,
verifiche funzionali/race del tracker, contatori Redis, snapshot cgroup e metriche.
Ogni directory di prova contiene CPU/allocs/heap`.pprof`, tabelle`top`/cumulative,
report di consegna e tempi effettivi dei profili. `analysis.json` riassume rate e
allocazioni. Nessun MONITOR o reset dei contatori sulla produzione.

Riferimenti al codice corrente:
[OnPublish e contabilizzazione byte](../../internal/broker/hooks.go),
[script byte](../../internal/forwarder/volume.go),
[persistenza e fencing dello schema](../../internal/broker/redis_session.go),
[shared subscription](../../thirdparty/mochi-mqtt-server/topics.go).
