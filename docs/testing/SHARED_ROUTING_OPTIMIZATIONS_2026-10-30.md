# Shared routing e contatore degli output — 30 ottobre 2026

Questa iterazione riduce il lavoro delle shared subscription e rimuove il
conteggio della quota di output quando nessun output connector è configurato.
Le metriche MQTT di messaggi e byte pubblicati restano attive; con connector
presenti il controllo della quota conserva il comportamento precedente.

Il routing tra edge usa `TopicsIndex.SubscriberIDs`: attraversa il trie MQTT
esistente e seleziona soltanto gli ID necessari, senza copiare tutte le
subscription e senza costruire le mappe di selezione completa. Mantiene i
wildcard, l'esclusione dei topic di sistema per i wildcard ordinari, una scelta
per filtro shared e la preferenza per il membro locale. Il normale metodo
`Subscribers` conserva gli snapshot modificabili dai hook.

La consegna shared, quando viene selezionato un solo client, tenta la consegna
prima di costruire il piano completo dei candidati. Se fallisce prima
dell'accettazione, costruisce il fallback riutilizzando l'esito del primo
tentativo: quel client non viene ritentato. Un errore ambiguo di persistenza o
trasporto interrompe il fallback. Le proprietà dei filtri sovrapposti vengono
ancora combinate, anche con selezioni fornite dai hook.

Microbenchmark Go 1.26.10, GOMAXPROCS=1, tre ripetizioni da 500 ms:

| Operazione | Byte/op | Allocazioni/op |
|---|---:|---:|
| Snapshot completo e selezione, 12 membri | 3.640 | 13 |
| Ricerca dei soli ID, 12 membri | 16 | 1 |
| Consegna shared prima, 12 candidati | 4.152 | 12 |
| Consegna shared dopo, 12 candidati | 1.416 | 5 |

Il microbenchmark della consegna misura un enqueue QoS 0 con un candidato
selezionato valido, senza writer di rete concorrente. Isola il costo del piano
di fallback; non misura throughput MQTT end-to-end. Le prime esecuzioni, con
una fixture che avviava un writer concorrente, sono state scartate e conservate
come diagnostica. La campagna completa Keel è separata da queste misure.

Verifiche:

- suite generale del modulo principale;
- broker e routing con Redis reale e race detector;
- pacchetto server del fork Mochi con race detector, inclusi i casi di
  saturazione, No Local, rollback e persistenza ambigua del fallback;
- test con Redis e PostgreSQL reali: nessuna richiesta Redis né lookup quota
  senza connector, metriche dei byte pubblicati conservate; con un connector,
  due payload da 3 byte e quota da 5 byte lasciano passare entrambi i publish
  MQTT ma inoltrano soltanto il primo all'output;
- 18 casi della suite cluster superati nella prima esecuzione, più il caso
  rolling verificato tre volte dopo la correzione delle sue precondizioni.

Il rolling iniziale falliva perché pubblicava prima della convergenza gossip.
La stessa condizione è stata riprodotta sull'immagine precedente alle ultime
ottimizzazioni: due fallimenti su tre esecuzioni, con errore `unknown node`
seguito dall'evento di join circa 60–80 ms dopo. Il test ora attende che gli
edge esistenti conoscano il sostituto e che l'ownership offline sia osservabile
prima di verificare il formato IFM. Non sono stati cambiati il timeout di
consegna o gli assert sul formato.

Questa correzione del test non elimina la finestra preesistente di routing
durante il join di un nodo. I benchmark a cluster convergente non dimostrano
assenza di perdita in quella finestra, né aumentano le garanzie della replica
asincrona Redis.

Artefatti e immagini immutabili:
`/home/fabio/keel-benchmark-lab/shared-optimizations-20261030`.
