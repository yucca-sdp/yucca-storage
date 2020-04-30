# Project Title
**Yucca Smart Data Platform** è una piattaforma cloud aperta e precompetitiva della Regione Piemonte, realizzata dal CSI Piemonte con tecnologie open source.
# Getting Started
La componente **speedalignment** del prodotto **yucca-storage** si occupa dell'allineamento da Hive a Solr; consisite in un pacchetto scala/yarn lanciato direttamente da wf oozie o spark submit.

# Prerequisites
I prerequisiti per l'installazione del prodotto sono i seguenti:
## Software
- [OpenJDK 8](https://openjdk.java.net/install/) o equivalenti
- [Apache Maven 3](https://maven.apache.org/download.cgi)
- yucca lib e admin api client

# Installing
## Istruzioni per la compilazione
- Da riga di comando eseguire `mvn -Dmaven.test.skip=true -P dev clean package`

# Versioning
Per la gestione del codice sorgente viene utilizzata la metodologia [Semantic Versioning](https://semver.org/).

# Authors
Gli autori della piattaforma Yucca sono:
- [Alessandro Franceschetti](mailto:alessandro.franceschetti@csi.it)
- [Claudio Parodi](mailto:claudio.parodi@csi.it)
# Copyrights
(C) Copyright 2020 Regione Piemonte
# License
Questo software è distribuito con licenza [EUPL-1.2-or-later](https://joinup.ec.europa.eu/collection/eupl/eupl-text-11-12)

Consulta il file [LICENSE.txt](LICENSE.txt) per i dettagli sulla licenza.
