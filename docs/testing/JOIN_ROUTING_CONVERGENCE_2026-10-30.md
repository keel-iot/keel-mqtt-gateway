# Forwarding durante la convergenza del join — 30 ottobre 2026

Le route distribuite tramite Olric possono arrivare al mittente prima del
metadato gossip che contiene l'indirizzo gRPC dell'edge destinatario. Il difetto
è stato osservato durante il rolling e riprodotto anche prima delle ultime
ottimizzazioni: l'inoltro falliva con `dataplane: unknown node`, seguito
dall'evento di join circa 60–80 ms dopo.

Il forwarding ora attende l'indirizzo se non è ancora noto, interrogando la
membership locale ogni 25 ms. La risoluzione e il successivo RPC condividono
il budget di tre secondi già esistente, o quello più breve del chiamante.
I target già noti richiedono una sola risoluzione immediata. I target del
fan-out restano concorrenti: una destinazione assente non ritarda l'invio
alle altre.

Viene ripetuta soltanto la ricerca dell'indirizzo prima dell'invio. Non viene
ritentato un RPC inviato che abbia restituito timeout o un altro errore,
perché il destinatario potrebbe aver già accettato il messaggio. Non vengono
introdotti queue persistenti, retry illimitati o nuove garanzie di consegna.

Test del dataplane con server gRPC reale e race detector:

- indirizzo inizialmente assente, poi disponibile: consegna una sola volta,
  preservando publish ID, topic, payload e QoS;
- indirizzo definitivamente assente: errore di nodo sconosciuto e deadline,
  rispettando sia il budget del chiamante sia quello predefinito;
- cancellazione mentre si attende: nessun invio dopo la cancellazione;
- nodo già noto: una sola chiamata al resolver;
- indirizzo vuoto: attesa di un indirizzo utilizzabile;
- RPC accettato ma risposta in timeout: una sola invocazione del destinatario.

La suite generale del modulo e i test broker/dataplane con race detector
sono passati. I test esistenti sul fan-out continuano a verificare budget
comune e concorrenza fra destinazioni.

Il caso di indirizzo mancante è coperto finché la membership converge entro
il budget. Un nodo che non compare entro quel tempo continua a fallire; una
route obsoleta non viene trasformata in una consegna garantita. Indirizzi
obsoleti/non raggiungibili, listener gRPC non pronto e perdite dopo l'invio
rimangono casi distinti. La correzione non dipende dallo schema IFM.

## Verifica con cluster reale

Sull'immagine candidata sono passati il traffico multinodo, la riconnessione
su un altro edge dopo un guasto e la prosecuzione QoS 2 dopo perdita dell'edge.
Il test di recupero offline QoS 1 ha segnalato un duplicato: la stessa
condizione è stata riprodotta sull'immagine precedente alla correzione
(`shared-optimization-20261030`), con due fallimenti su tre esecuzioni.
La suite cluster selezionata non viene quindi dichiarata interamente verde.
Il duplicato resta un finding distinto e non viene corretto cambiando le
asserzioni del test o introducendo retry di RPC.

La regressione deterministica route-prima-di-membership usa un server gRPC
reale e controlla la disponibilità dell'indirizzo dal lato mittente. Non forza
ritardi gossip tramite flag di produzione e non assume che una ripetizione
casuale del join riproduca sempre la finestra.

La ripetizione del recupero offline sull'immagine candidata ha dato lo stesso
esito della baseline: un successo e due duplicati su tre. Questo confronto
non dimostra la causa del duplicato; dimostra che il finding è riproducibile
anche senza la nuova attesa del resolver. I log sono conservati in
`/home/fabio/keel-benchmark-lab/join-resolution-20261030`.

Aggiornamento successivo: la causa del duplicato offline è stata identificata
e corretta separatamente; vedere
[analisi e verifica del doppio accodamento](OFFLINE_DUPLICATE_ROOT_CAUSE_2026-10-30.md).
La nuova suite mirata di otto casi cluster, che include il caso deterministico
prima fallente, passa dopo quella correzione.
