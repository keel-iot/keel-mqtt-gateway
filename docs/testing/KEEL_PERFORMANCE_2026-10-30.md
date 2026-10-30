# Benchmark completi Keel — 30 ottobre 2026

Campagna completata su nodo singolo e cluster: **39 prove**, di cui 32 con traffico e sette idle contando la ripetizione diagnostica. Le prove attive hanno consegnato tutti i **5.616.000 messaggi** attesi, senza messaggi mancanti, duplicati o errori. Il ritmo richiesto è sostenuto fino al punto da 6.400 publisher in entrambe le topologie e per tutti i QoS. Questo è l’ultimo gradino qualificato della scala, non una misura della capacità massima del broker.

[Tutti i risultati CSV](data/keel-performance-2026-10-30.csv) · [Modifiche shared e quote](SHARED_ROUTING_OPTIMIZATIONS_2026-10-30.md) · [Campagna precedente](BROKER_PERFORMANCE_2026-10-29.md)

Immagine locale verificata in tutti i campioni: `sha256:d3db4917e56668586217c2ad1cabb18f4c7a6a92be459cf435b7ef500abd18bb`. Nessuna nuova release è stata pubblicata. I confronti storici includono le ottimizzazioni di questa iterazione e quelle locali precedenti; non isolano il contributo di una singola modifica.

## Metodo

Stesso PC Ubuntu 24.04 / Core Ultra 7 155H e Raspberry Pi 4 ARM64 con 4 GB via Ethernet. MQTT 3.1.1, payload sintetico da 256 byte, clean session, retained disabilitato, 12 consumer shared, un messaggio/s per publisher. QoS 0/1/2 simmetrico tra publisher e subscriber. Scala attiva: 1.600, 3.200, 6.400 e 12.800 publisher, warmup 20 s e misura 30 s; la crescita termina al primo punto non qualificato. Una ripetizione per punto: le differenze piccole, soprattutto sulla latenza, richiedono ulteriori ripetizioni per una conclusione statistica.

La campagna principale usa il generatore Go verificato. Una scala QoS 1 aggiuntiva usa Python per riprodurre il riferimento single-node precedente. Python ha warmup attivo; Go tiene aperte le connessioni senza traffico durante il warmup. Le due modalità sono separate e non confrontate come se fossero identiche.

Single-node: broker con 2 CPU e 4 GiB, GOMAXPROCS=2, modalità di conformance, senza Redis; PostgreSQL rimane nel setup. Cluster: tre core da 0,25 CPU, tre Redis con AOF da 0,75 CPU e tre edge da 1 CPU, GOMAXPROCS=1. Backlog Redis 128 MiB. Autenticazione file e ACL abilitate. I publisher entrano su un solo edge, i consumer sugli altri due: questa topologia non misura il massimo del cluster con ingressi bilanciati. Nessun output connector, TLS o uscita Kafka.

Il primo gruppo di misure cluster usa schema IFM 1, marker assente interpretato in modalità auto. Due scale aggiuntive attivano esplicitamente il marker 2 nel setup del solo laboratorio e verificano QoS 1/2 a 3.200 e 6.400 publisher. Non è un’attivazione automatica da configurazione di startup.

CPU: media cgroup nella finestra di misura, espressa in core. RAM: picco di memory.current nella stessa finestra, comprensivo di cache e memoria kernel. Le colonne stack escludono PostgreSQL; il CSV conserva il suo consumo e il dettaglio per container. L’allineamento degli orologi è ricalibrato via SSH persistente. Nessun profiler CPU/heap è attivo durante queste prove.

## Traffico a 6.400 messaggi/s, generatore Go

Tutti i punti nella tabella hanno ricevuto 192.000/192.000 messaggi senza errori o duplicati.

| Topologia | QoS | CPU stack, core | RAM stack, MiB | p99 end-to-end, ms |
|---|---:|---:|---:|---:|
| Singolo | 0 | 0.337 | 492.4 | 64.96 |
| Singolo | 1 | 0.456 | 495.9 | 64.40 |
| Singolo | 2 | 0.665 | 496.8 | 105.18 |
| Cluster, IFM 1 | 0 | 0.918 | 633.6 | 80.48 |
| Cluster, IFM 1 | 1 | 2.168 | 1034.2 | 124.68 |
| Cluster, IFM 1 | 2 | 2.841 | 1070.1 | 136.50 |

Nel caso cluster QoS 2 il singolo edge di ingresso usa circa 0,93 della CPU assegnata, il container Redis primario circa 0,58 core. La CPU totale include gli altri edge, core e repliche. Tutti i contatori di output giornalieri campionati dopo le prove cluster sono assenti, come previsto quando nessun connector è configurato.

## Confronto con la campagna precedente, QoS 1 a 6.400/s

Valori storici dal report del 29 ottobre, con medesima topologia, generatore e limiti. Sono confronti esplorativi sequenziali, non coppie randomizzate o intervalli di confidenza.

| Profilo | CPU prima → dopo, core | Variazione CPU | RAM prima → dopo, MiB | p99 prima → dopo, ms |
|---|---:|---:|---:|---:|
| Singolo, Python | 0.505 → 0.471 | -6.8% | 485.8 → 492.8 | 6.93 → 6.88 |
| Singolo, Go | 0.479 → 0.456 | -4.7% | 477.0 → 495.9 | 62.08 → 64.40 |
| Cluster, Go | 2.906 → 2.168 | -25.4% | 1036.8 → 1034.2 | 82.83 → 124.68 |

Il miglioramento più evidente è nella CPU del cluster, circa il 25% in meno. La memoria residente resta sostanzialmente invariata. La latenza non migliora in modo uniforme: nel campione cluster la p99 è più alta, pur rimanendo sotto la soglia di qualificazione della prova. La diversa latenza fra Python e Go mostra anche quanto conti il generatore.

## Schema IFM 2

| QoS | Publisher / msg/s | Ricevuti / attesi | CPU stack, core | RAM stack, MiB | p99, ms |
|---|---:|---:|---:|---:|---:|
| 1 | 3200 | 96000/96000 | 1.344 | 578.8 | 290.40 |
| 1 | 6400 | 192000/192000 | 2.220 | 1068.6 | 166.81 |
| 2 | 3200 | 96000/96000 | 1.891 | 683.3 | 31.54 |
| 2 | 6400 | 192000/192000 | 2.880 | 1042.3 | 140.60 |

Entrambi i carichi per entrambi i QoS rispettano i criteri della campagna, senza perdite, duplicati o errori.

## Punti a 12.800/s

Le sette scale attive complete (sei Go e il riferimento Python) hanno consegnato il totale finale atteso anche a 12.800 publisher, ma tutte mostrano ritardo d’invio. Nel cluster QoS 1/2 sono presenti anche backlog e p99 superiore a un secondo. Questi punti non sono qualificati come ritmo sostenuto. Il flag generator_limited misura lateness e non individua da solo il collo di bottiglia: può includere backpressure del broker. Non è corretto assegnare tutto il limite al Raspberry né dedurne la capacità massima di Keel.

## Connessioni inattive

La scala idle usa tre endpoint per evitare il limite delle porte TCP sorgenti su una singola coppia sorgente/destinazione. Sul nodo singolo tutti gli endpoint portano allo stesso processo; nel cluster portano ai tre edge. I valori indicano publisher inattivi e ci sono altri 12 consumer connessi. Si tratta di prove brevi, non di un test di durata.

| Topologia / esecuzione | Publisher inattivi | Aperture / attese | Errori | CPU stack | RAM stack, MiB |
|---|---:|---:|---:|---:|---:|
| Singolo | 12800 | 12812/12812 | 0 | 0.031 | 494.2 |
| Singolo | 25600 | 25612/25612 | 0 | 0.054 | 951.1 |
| Singolo | 51200 | 51212/51212 | 0 | 0.100 | 1915.0 |
| Cluster, scala progressiva | 12800 | 12812/12812 | 0 | 0.071 | 625.8 |
| Cluster, scala progressiva | 25600 | 25612/25612 | 0 | 0.112 | 1305.2 |
| Cluster, scala progressiva | 51200 | 51211/51212 | 1 | 0.182 | 2503.2 |
| Cluster, ripetizione da pulito | 51200 | 51212/51212 | 0 | 0.175 | 2317.1 |

Il primo punto cluster da 51.200 ha un rifiuto di connessione not Authorized; tutti i container risultavano ancora in esecuzione e senza OOM. Non viene classificato come passato. La ripetizione dello stesso punto da cluster pulito apre tutte le 51.212 connessioni senza errori. Il primo risultato resta conservato: la causa del rifiuto sporadico non è accertata. Gli errori DNS/RPC osservati dopo lo spegnimento dei core non vengono utilizzati come spiegazione del rifiuto precedente. Non attribuiamo automaticamente l’episodio alle ottimizzazioni, né lo cancelliamo con il retry positivo.

## Correttezza e limiti per il rilascio

La suite generale, il race detector sul broker/routing e sul server Mochi, i test quota con Redis/PostgreSQL reali e i test cluster sono descritti nel report delle modifiche. I 18 scenari cluster passati inizialmente sono affiancati dal rolling ripetuto tre volte dopo aver corretto le precondizioni di convergenza del test.

Resta il limite preesistente durante il join: una route può essere osservabile prima che il sender conosca l’indirizzo gRPC del nuovo nodo. È stato riprodotto anche sull’immagine precedente alle ultime ottimizzazioni. Le prove a cluster convergente non dimostrano assenza di perdita in quella finestra. Questo punto e il rifiuto sporadico al livello idle massimo vanno mantenuti visibili nella valutazione del rilascio. Non sono cambiati i limiti della replica asincrona Redis.

Gli interventi verificati sono il percorso shared con meno copie/allocazioni e l’esclusione del conteggio quota quando gli output connector sono assenti. Le quote dei connector presenti mantengono il comportamento precedente. L’eventuale aggregazione dei contatori nei core e la loro persistenza periodica sono lavoro successivo.

Artefatti riproducibili, configurazioni, immagini, sorgenti del generatore, report JSON, risorse cgroup e log: `/home/fabio/keel-benchmark-lab/keel-complete-20261030`. Tutti i container e i generatori del laboratorio vengono fermati al termine. Le modifiche rimangono locali in attesa della valutazione del rilascio.

## Chiusura del ciclo

Il ciclo è chiuso su richiesta dell'utente. Dopo i benchmark sono passati
nuovamente `go test ./...` nel modulo principale e nel modulo del fork Mochi,
oltre ai 13 test Python del classificatore dei risultati di conformità.
Non sono stati ripetuti altri carichi né modificati i parametri per cancellare
i punti non qualificati. I processi del laboratorio risultano arrestati.
Codice, test e report sono preparati per il commit locale; versione, tag,
push e pubblicazione restano operazioni successive.
