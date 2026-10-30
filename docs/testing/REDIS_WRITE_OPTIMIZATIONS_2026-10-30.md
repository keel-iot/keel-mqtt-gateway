# Ottimizzazioni Redis e allocazioni — 30 ottobre 2026

Il confronto controllato mostra un miglioramento del percorso QoS 2 senza errori di consegna. A 3.200 messaggi/s la CPU dell’edge di ingresso scende di circa il 10%, quella del processo master Redis di circa il 9%. Le allocazioni sull’edge di ingresso scendono di circa il 10%; la memoria residente non mostra una riduzione comparabile.

Le modifiche di questa iterazione sono:

- eliminazione della serializzazione duplicata dello stesso inflight;
- salvataggi e cancellazioni inflight tramite controllo atomico del marker nella stessa richiesta Redis, senza GET separato nel percorso stabile;
- esclusione della costruzione di richieste ed eventi per output connector quando non ne è configurato nessuno;
- selezione esplicita dello schema allo startup, senza attivazione automatica della migrazione.

Il confronto parte dallo snapshot locale già usato nella profilatura precedente, con le modifiche precedenti sul fork Mochi e sul percorso shared. Non è un confronto diretto con la release 0.3.2.

| Immagine | ID immutabile |
|---|---|
| Prima | `sha256:f957f435352b978a66b42c462ec1d11a50a2a5e6f20fdc293a0b98f0700b0423` |
| Dopo | `sha256:199df03a9b757c7c698be1d4348f8487c4f98e430e4cf816db3a8c306d0d7521` |

Il laboratorio usa lo stesso PC Ubuntu e il Raspberry Pi 4 via Ethernet, tre core, tre Redis con AOF e tre edge. Limiti invariati: 1 CPU per edge, 0,75 CPU per Redis. Tutti i publisher entrano sul primo edge; 12 consumer shared sono sugli altri due. MQTT 3.1.1, clean session, payload 256 byte, retained disabilitato, QoS simmetrico publisher/subscriber. Autenticazione file e nessun output connector. Il marker IFM è assente: lo schema effettivo è 1 in modalità auto, come confermato dai log di avvio. Il laboratorio non attiva automaticamente lo schema 2.

Ogni caso dura 60 secondi dopo il warmup. CPU e allocazioni sono profilate per 25 secondi; vengono raccolti anche heap, contatori Redis, risorse cgroup e risultati del generatore. Una singola coppia prima/dopo per caso fornisce un’indicazione sperimentale, non una stima della variabilità o una garanzia per produzione.

| Caso | Versione | CPU ingresso, core | CPU master Redis, core | Allocazioni ingresso, MiB/s | Allocazioni tutti gli edge, MiB/s |
|---|---|---:|---:|---:|---:|
| 3200, QoS 0 | Prima | 0.342 | 0.112 | 41.7 | 82.0 |
| 3200, QoS 0 | Dopo | 0.345 | 0.114 | 39.3 | 81.1 |
| 3200, QoS 2 | Prima | 0.797 | 0.547 | 58.1 | 120.2 |
| 3200, QoS 2 | Dopo | 0.718 | 0.496 | 52.2 | 107.4 |
| 6400, QoS 2 | Prima | 0.943 | 0.648 | 102.0 | 203.4 |
| 6400, QoS 2 | Dopo | 0.933 | 0.639 | 106.3 | 217.7 |

La CPU ingresso è la media cgroup della finestra di traffico; la CPU Redis deriva dai delta INFO e non include i processi figli. Le allocazioni sono stime pprof, non memoria residente. Il caso a 6.400 non va confrontato a parità di lavoro: dopo le modifiche viene elaborato più traffico nella finestra. A QoS 0 la CPU resta sostanzialmente invariata. Il conteggio giornaliero dei byte rimane attivo anche a QoS 0.

A 3.200 messaggi/s QoS 2, Redis passa da circa 47.938 a 48.066 comandi/s. I GET del marker, inclusi quelli interni a Lua, passano da circa 6 a circa 4 per messaggio, mentre gli EVALSHA passano da circa 3 a circa 5: le cancellazioni diventano script atomici. Il vantaggio deriva dalle richieste client risparmiate e dalla minore serializzazione, non da una riduzione del totale dei comandi contati dal server.

Tutte le sei prove consegnano il totale atteso, senza errori, messaggi mancanti o duplicati. A 3.200 publisher il ritmo è rispettato sia prima sia dopo. A 6.400 publisher il totale atteso è 384.000 messaggi:

| Versione | Inviati entro 60 s | Ricevuti entro 60 s | Tempo finale di smaltimento |
|---|---:|---:|---:|
| Prima | 325,386 | 321,793 | 13.48 s |
| Dopo | 380,744 | 378,296 | 2.41 s |

Le consegne nella finestra aumentano di circa il 17,6%. Entrambe le prove a 6.400 hanno lateness e il flag generator_limited: il carico richiesto non è ancora pienamente sostenuto. Questo flag non identifica da solo il componente che limita il sistema. Nel caso modificato l’ingresso usa circa 0,93 della CPU assegnata e il container Redis mostra circa 1,68 secondi di throttling nella finestra; il generatore usa circa 2 core. Non attribuiamo il limite esclusivamente al Raspberry.

La configurazione è disponibile tramite ambiente o Helm:

```yaml
core:
  redis:
    ifmSchema: "auto"
```

`REDIS_IFM_SCHEMA=auto|1|2`: auto è il default e segue l’attivazione; 1/2 selezionano uno schema esistente e fanno fallire lo startup se il marker non coincide. Marker assente significa schema 1. Dichiarare 2 non attiva la migrazione. Durante una transizione usare auto su tutti i writer; le mutazioni con schema esplicito incompatibile vengono rifiutate. Il controllo atomico nelle operazioni Redis resta attivo in tutte le modalità. I log di avvio del broker indicano schema e modalità.

Verifiche completate:

- suite generale `go test ./...`;
- broker con Redis reale, configurazione e server con race detector;
- sette test di cluster: failover leader core, rolling reale 0.3.2 → compatibile, recupero offline su altro edge, collisioni packet ID live/offline, handshake QoS 2 dopo perdita edge, 20 consumer shared senza crescita offline e resume shared sullo stesso edge;
- Helm lint e rendering di default auto e schema esplicito 2 su core/edge;
- sei prove di carico con 90 profili CPU/allocazioni/heap complessivi.

I test simulano gli scenari descritti e non dimostrano assenza di perdita in ogni tipo di guasto. Non vengono cambiate le garanzie della replica asincrona Redis o la politica di persistenza. Non viene attivato lo schema 2 né effettuata pulizia dei dati di produzione.

Gli artefatti completi sono in `/home/fabio/keel-benchmark-lab/write-optimizations-20261030`: manifest, immagini, fixture, script riproducibili, risultati di consegna, contatori Redis, profili e log dei test. La prima preparazione del laboratorio è fallita per fixture non copiate nella nuova directory; corretta la preparazione, tutte le misure sono state eseguite. Il log iniziale è conservato in `failed-fixture-setup.log`. Il laboratorio viene fermato al termine.
