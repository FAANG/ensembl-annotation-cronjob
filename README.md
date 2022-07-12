# ensembl-annotation-cronjob
Python script (web scraping) running as a cron job to load the ensembl-annotation index

Job runs every Monday at 01.00 AM to load data into the ensembl-annotation index. Data is read from:

gene-switch - https://projects.ensembl.org/gene-switch/

aqua-faang - https://projects.ensembl.org/aqua-faang/

bovreg - https://projects.ensembl.org/bovreg/
