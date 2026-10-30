# Broker performance, 29 ottobre 2026

Campagna QoS1 conclusa. Misure esplorative locali: una ripetizione per punto,
con ripetizioni mirate per i casi anomali e le correzioni del setup. I risultati non sono
una classifica di capacità massima e non rappresentano il cluster di produzione.

## Metodo

PC Ubuntu 24.04, Core Ultra 7 155H, circa 30 GiB RAM. Generatore su Raspberry Pi 4
con 4 GiB RAM, Debian 13 ARM64, Ethernet. Baseline rete separata: 899/932 Mbit/s.
MQTT 3.1.1, QoS1, payload sintetico da 256 byte, sessioni pulite, nessun retained,
12 consumer in una shared subscription, un messaggio/s per publisher.
Il profilo Python ha 20 secondi di riscaldamento attivo alla stessa frequenza
(sequenze sentinel escluse dai conteggi misurati), 30 secondi di misura
e fino a 20 secondi di drain; tutti i messaggi attesi hanno sequenze verificate.
Niente TLS, trasformazioni o uscita Kafka. QoS0 e QoS2 sono campagne successive;
per questi QoS sono state già completate solo le verifiche funzionali del generatore.

Single-node: ciascun broker ha quota 2 CPU, limite 4 GiB, swap container disabilitata.
La memoria è `memory.current` del cgroup (include cache/kernel), con massimo dei
campioni nella finestra di traffico. CPU: delta `usage_usec` sulla stessa finestra.
Il generatore comune single-node QoS1 è Python multiprocesso verificato.
Il tempo di avvio e di connessione è esterno alla finestra di traffico.
La latenza è end-to-end e include client e rete; non è il tempo del solo broker.

Keel standalone usa `--conformance-test`, con autenticazione/ACL bypassate dal
provider di test, e nessun Redis. PostgreSQL resta richiesto dal cablaggio attuale.
Keel è l'immagine del working tree già validato nei test cluster, non una nuova
release pubblicata. Mochi upstream e fork usano lo stesso programma di avvio,
stesso compilatore Go 1.26.10, TCP e hook AllowAll; nessun database o Redis.
I moduli Mochi dichiarano Go 1.21: i valori predefiniti del runtime non rispettano
automaticamente la quota CPU come quelli di un main module Go >=1.25. La campagna
ripete Mochi con GOMAXPROCS=2 esplicito; i campioni precedenti restano nel CSV e
sono distinguibili dal campo gomaxprocs_environment. Fonte: [Go runtime e cgroup](https://go.dev/blog/container-aware-gomaxprocs).

## Single-node, fino a 6.400 publisher / messaggi al secondo

| Broker | Carico msg/s | Ricevuti / attesi | Persi | Duplicati | p99 ms | CPU stack | RAM stack MiB | CPU PostgreSQL | RAM PostgreSQL MiB | Qualifica |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Mosquitto 2.1.2 | 6400 | 192000/192000 | 0 | 0 | 8.94 | 0.143 | 46.2 | 0.000 | 0.0 | completato |
| Mochi upstream 2.7.9 | 6400 | 192000/192000 | 0 | 0 | 6.12 | 0.398 | 1185.1 | 0.000 | 0.0 | completato |
| Mochi fork locale | 6400 | 192000/192000 | 0 | 0 | 6.53 | 0.448 | 479.8 | 0.000 | 0.0 | completato |
| Keel snapshot locale | 6400 | 192000/192000 | 0 | 0 | 6.93 | 0.505 | 485.8 | 0.013 | 23.2 | completato |
| EMQX 6.3.2 | 6400 | 192000/192000 | 0 | 0 | 91.12 | 0.709 | 707.9 | 0.000 | 0.0 | completato |
| EMQX OSS 5.8.9 | 6400 | 192000/192000 | 0 | 0 | 6.88 | 0.699 | 540.1 | 0.000 | 0.0 | completato |
| HiveMQ CE 2026.5 | 6400 | 192000/192000 | 0 | 0 | 45.33 | 0.301 | 222.9 | 0.000 | 0.0 | completato |
| VerneMQ 2.2.1 | 6400 | 192000/192000 | 0 | 0 | 6.50 | 1.627 | 1707.0 | 0.000 | 0.0 | completato |
| RMQTT 0.24.0 | 6400 | 192000/192000 | 0 | 0 | 11.08 | 0.317 | 206.1 | 0.000 | 0.0 | completato |
| FlashMQ 1.27.3 | 6400 | 192000/192000 | 0 | 0 | 10.99 | 0.197 | 44.6 | 0.000 | 0.0 | completato |
| NanoMQ 0.25.7 | 6400 | 192000/192000 | 0 | 0 | 7.27 | 1.689 | 94.1 | 0.000 | 0.0 | completato |
| rumqttd 0.20.0 | 6400 | 192000/192000 | 0 | 0 | 121.32 | 0.292 | 735.8 | 0.000 | 0.0 | completato |

`Carico msg/s` è esplicito: se una scala si interrompe prima di 6.400, il campione
mostrato ha un carico minore e non si confrontano direttamente i consumi.
`CPU stack` e `RAM stack` escludono PostgreSQL, che è esposto separatamente.
Il CSV contiene anche il consumo combinato e il dettaglio CPU per container.
Con AUTH_BACKEND=file le credenziali sono lette dal file e `last_seen` è un no-op.
Le ACL del cluster risiedono in Raft. Tuttavia il binario attuale inizializza
sempre PostgreSQL, esegue le migrazioni e usa TenantConfigCache per le policy dei
tenant. Quindi file auth non elimina ancora la dipendenza dal DB all'avvio.

## Calibrazione del Raspberry

- Python: a 6.400/s mantiene il profilo; a 12.800/s i due publisher saturano un
  core ciascuno, ritardo d'invio p99 circa 1.164 ms. Tutti i messaggi arrivano,
  ma quel punto non dimostra un limite di Mosquitto.
- Go/Paho: riduce CPU rispetto a Python, ma a 12.800/s supera comunque la soglia
  di pianificazione (p99 circa 253 ms). Non si mescolano le due curve.
- Sole connessioni: 25.612 e 51.212 client totali, comprese 12 subscription,
  con una finestra di osservazione di 30 secondi per punto. Nessun errore o swap.
  A 51.200 publisher il processo generatore arriva a circa 1.930 MiB RSS e usa
  circa 0,44 CPU durante la finestra; MemAvailable del Pi resta >=1.289 MiB.
  Questa non è una prova di 51.200 messaggi/s né un test di durata.
- Tre porte host sono dirette alla medesima istanza Mosquitto per superare il
  vincolo delle porte sorgente 32768..60999 senza modificare sysctl.

## Cluster

Risultati separati: container sullo stesso PC, nessuna simulazione di macchine
fisiche indipendenti o partizioni di rete. Il generatore Go/Paho distribuisce
solo i consumer sui nodi 2 e 3 e mette tutti i publisher sul nodo 1, per forzare
l'inoltro tra nodi. Le latenze non si sottraggono direttamente a quelle del
profilo single-node Python; serve un riferimento single-node Go equivalente.
Keel conserva autenticazione da file, ACL, Redis con AOF/repliche, Raft e PostgreSQL.
Quindi le garanzie di persistenza differiscono dai broker configurati in memoria.
Tre broker hanno in totale 6 CPU/12 GiB di limiti; Keel core/Redis/edge ha gli
stessi totali, più 0,5 CPU/512 MiB PostgreSQL, contabilizzati separatamente.

Il riscaldamento di questo profilo Go è di attesa senza traffico, a differenza
del riscaldamento attivo Python. Il nodo di ingresso ha 2 CPU negli altri broker,
1 CPU per Edge in Keel: il budget totale di Keel è ripartito anche tra core e Redis.
Queste sono prove di inoltro con un solo ingresso, non della capacità aggregata
con publisher uniformemente distribuiti su tutti i nodi.

| Cluster | Carico msg/s | Ricevuti/attesi | Persi | p99 ms | CPU stack | RAM stack MiB | CPU PostgreSQL | Qualifica |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| emqx-oss-cluster | 6400 | 192000/192000 | 0 | 62.89 | 1.173 | 1035.6 | 0.000 | completato |
| vernemq-cluster | 6400 | 192000/192000 | 0 | 65.04 | 3.356 | 3413.6 | 0.000 | completato |
| rmqtt-cluster | 6400 | 192000/192000 | 0 | 128.07 | 0.992 | 267.3 | 0.000 | completato |
| keel-cluster | 6400 | 192000/192000 | 0 | 82.83 | 2.906 | 1036.8 | 0.015 | completato |

### Riferimenti single-node con il medesimo generatore Go

| Broker | Carico msg/s | Ricevuti/attesi | Persi | p99 ms | CPU stack | RAM stack MiB | Qualifica |
|---|---:|---:|---:|---:|---:|---:|---|
| Keel snapshot locale | 6400 | 192000/192000 | 0 | 62.08 | 0.479 | 477.0 | completato |
| EMQX OSS 5.8.9 | 6400 | 192000/192000 | 0 | 114.92 | 0.682 | 551.1 | completato |
| VerneMQ 2.2.1 | 6400 | 192000/192000 | 0 | 95.49 | 1.488 | 1502.6 | completato |
| RMQTT 0.24.0 | 6400 | 192000/192000 | 0 | 85.34 | 0.339 | 178.8 | completato |
| HiveMQ CE 2026.5 | 6400 | 191722/192000 | 278 | 120.82 | 0.284 | 220.0 | delivery_or_protocol_failure |

## Edizioni con licenza e versioni

HiveMQ Enterprise e il cluster EMQX recente sono inclusi nella matrice richiesta,
ma non ancora eseguiti: non è stata fornita una licenza/trial adeguata.
Il trial HiveMQ integrato ammette 25 connessioni, non il carico comune.
La licenza Community EMQX recente è single-node. Non sono state fatte iscrizioni,
acquisti o aggiramenti dei limiti. VerneMQ è compilato dai sorgenti Apache.

Fonti primarie: [HiveMQ licenze](https://docs.hivemq.com/hivemq-control-center/latest/licenses.html),
[EMQX licenze](https://docs.emqx.com/en/emqx/latest/get-started/deploy/license.html),
[VerneMQ sorgenti e pacchetti](https://github.com/vernemq/docker-vernemq),
[Mosquitto download](https://mosquitto.org/download/).
Il tag Docker Mosquitto 2.1.2 non era disponibile: l'immagine è stata costruita
dai sorgenti del tag v2.1.2, conservando commit e log di compilazione.

## Riproducibilità e artefatti

Laboratorio: `/home/fabio/keel-benchmark-lab/comparison-20261029`.

- `matrix.json`: intera matrice richiesta, inclusi i vincoli di licenza.
- `smoke-summary.json`: verifiche funzionali a basso carico, non benchmark.
- `run-*/source`, JSON dei trial, log e campioni cgroup: configurazioni effettive,
  immagini e relativi ID, sorgenti/hash del generatore, errori e criteri di stop.
- `results.csv` e `results.json`: tutte le scale, con generatore e tipo di prova.
- `pi-calibration.json`: memoria, swap, CPU e ritardi del generatore.
- `source-sha256.json`, build/release metadata: provenienza delle build locali.

Osservazioni da mantenere accanto ai campioni più recenti:

- EMQX OSS nel primo Python a 6.400 msg/s: 2.981 mancanti, warning coda client piena
  (limite predefinito 1.000). Una ripetizione successiva ha consegnato tutti i messaggi;
  il primo episodio non dimostra una soglia di capacità riproducibile.
- HiveMQ CE nel primo Python a 1.600 msg/s: 620 mancanti; nel controllo Paho a 6.400 msg/s: 278
  mancanti. Il secondo event.log contiene esattamente 278 scarti per quel run,
  motivo coda shared piena. Una successiva ripetizione Python completa ha
  consegnato tutti i messaggi fino a 6.400/s: gli scarti non sono deterministici.
  Le prestazioni con shared non si estendono alle subscription normali.
- Primo tentativo VerneMQ cluster: membership visibile, ma comunicazione dati non funzionante;
  bind di clustering corretto su indirizzi espliciti prima della ripetizione.
- Le correzioni agli script di setup (nomi Erlang FQDN, bind VerneMQ espliciti,
  peer RMQTT da file e leader iniziale esplicito per il laboratorio,
  risposta HTTP vuota delle ACL Keel) non sono modifiche al codice dei broker.

I risultati incompleti o esclusi non sono sostituiti con numeri di altri prodotti.
