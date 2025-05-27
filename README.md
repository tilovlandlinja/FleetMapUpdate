# OppdateMontor

Dette prosjektet automatiserer oppdateringer og synkroniseringer for ulike datasett og tjenester. Prosjektet er satt opp til å kjøre som cronjobber på serveren `10.182.86.34` (Adms Ubuntu).

## Innhold

- Automatisert oppdatering fra SOAP-tjenester
- Oppdatering av kartdata fra database
- Integrasjon mot Eviny
- Daglig oppdatering av kortdata

## Katalogstruktur

- `update_fromsoap.py` – Henter og oppdaterer data fra SOAP-tjenester.
- `updatemapfromdb.py` – Oppdaterer kartdata basert på databaseinformasjon.
- `updateeviny.py` – Integrasjon og oppdatering mot Eviny.
- `updateskort.py` – Daglig oppdatering av kortdata.
- `venv/` – Virtuelt Python-miljø for avhengigheter.
- `cron.log` – Loggfil for cronjobber.

## Cronjobber

Følgende cronjobber er satt opp for å kjøre skriptene automatisk:

```cron
* * * * * cd /toringe/OppdateMontor && /toringe/OppdateMontor/venv/bin/python update_fromsoap.py >> /toringe/OppdateMontor/cron.log 2>&1
* * * * * cd /toringe/OppdateMontor && /toringe/OppdateMontor/venv/bin/python updatemapfromdb.py >> /toringe/OppdateMontor/cron.log 2>&1
* * * * * cd /toringe/OppdateMontor && /toringe/OppdateMontor/venv/bin/python updateeviny.py >> /toringe/OppdateMontor/cron.log 2>&1
0 0 * * * cd /toringe/OppdateMontor && /toringe/OppdateMontor/venv/bin/python updateskort.py >> /toringe/OppdateMontor/cron.log 2>&1
```

- De tre første skriptene kjøres hvert minutt.
- `updateskort.py` kjøres daglig ved midnatt.

## Oppsett

1. Klon prosjektet til ønsket katalog på serveren.
2. Opprett og aktiver et virtuelt miljø:
    ```sh
    python3 -m venv venv
    source venv/bin/activate
    ```
3. Installer nødvendige avhengigheter (se eventuelt `requirements.txt`).
4. Legg til cronjobbene i crontab for brukeren som skal kjøre skriptene:
    ```sh
    crontab -e
    ```
    og lim inn linjene over.

## Logging

Alle cronjobber logger til `cron.log` i prosjektmappen.

---

For spørsmål, kontakt systemansvarlig.