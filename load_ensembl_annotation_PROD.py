import requests
import bs4
import os
from dotenv import load_dotenv


# connect to our ES cluster
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection

# load .env variables
load_dotenv()

# read from k8s secrets
ES_USERNAME = os.getenv('ES_USER')
ES_PASSWORD = os.getenv('ES_PASSWORD')
ES_HOST = os.environ.get("ES_NODE")


es = Elasticsearch([ES_HOST],
                   connection_class=RequestsHttpConnection,
                   http_auth=(ES_USERNAME, ES_PASSWORD),
                   # http_auth=('elastic', 'cD7351AoJa559BHDT53ntBY5'),
                   use_ssl=True, verify_certs=False,
                   ssl_show_warn=False)


def scrape_data(project_name):
    result = ''
    if project_name == 'GENE-SWitCH':
        result = requests.get('https://projects.ensembl.org/gene-switch/')

    if project_name == 'AQUA-FAANG':
        result = requests.get('https://projects.ensembl.org/aqua-faang/')

    if project_name == 'BovReg':
        result = requests.get('https://projects.ensembl.org/bovreg/')

    if result:
        soup = bs4.BeautifulSoup(result.text, "lxml")
        table = soup.find('table', class_='table_zebra')

        # Collecting data
        for row in table.tbody.find_all('tr'):
            # Find all data for each column
            columns = row.find_all('td')

            if (columns != []):
                species = columns[0].text.strip()
                accession = columns[1].text.strip()
                assembly_submitter = columns[2].text.strip()

                # annotation column
                annotation_list = []
                annotation_spans = columns[3].find_all('span')
                for span in annotation_spans:
                    annotation_list.append({"annotation": span.a.text.strip(),
                                            "fileUrl": span.a['href']
                                            })

                # proteins column
                proteins_list = []
                protein_links = columns[4].find_all('a')
                for a in protein_links:
                    proteins_list.append({"fileType": a.text.strip(),
                                          "fileUrl": a['href']
                                          })

                # transcripts column
                transcripts_list = []
                transcripts_links = columns[5].find_all('a')
                for a in transcripts_links:
                    transcripts_list.append({"fileType": a.text.strip(),
                                             "fileUrl": a['href']
                                             })

                # softmaskedgenome column
                softmaskedgenome_list = []
                softmaskedgenome_links = columns[6].find_all('a')
                for a in softmaskedgenome_links:
                    softmaskedgenome_list.append({"fileType": a.text.strip(),
                                                  "fileUrl": a['href']
                                                  })

                # repeatlibrary column
                repeatlibrary_list = []
                repeatlibrary_links = columns[7].find_all('a')
                for a in repeatlibrary_links:
                    repeatlibrary_list.append({"library": a.text.strip(),
                                               "fileUrl": a['href']
                                               })

                # otherdata column
                otherdata_list = []
                otherdata_links = columns[8].find_all('a')
                for a in otherdata_links:
                    otherdata_list.append({"otherData": a.text.strip(),
                                           "fileUrl": a['href']
                                           })

                # view_in_browser column
                browserview_list = []
                browserview_links = columns[9].find_all('a')
                for a in browserview_links:
                    browserview_list.append({"browserView": a.text.strip(),
                                             "fileUrl": a['href']
                                             })

                # create a document to upload
                data = {'species': species,
                        'accession': accession,
                        'assembly_submitter': assembly_submitter,
                        'annotation': annotation_list,
                        'proteins': proteins_list,
                        'transcripts': transcripts_list,
                        'softmasked_genome': softmaskedgenome_list,
                        'repeat_library': repeatlibrary_list,
                        'other_data': otherdata_list,
                        'browser_view': browserview_list,
                        'project': project_name}

                # load document in index
                load_ensembl_annotation(data)


def load_ensembl_annotation(data):
    try:
        # add document to index
        res = es.index(index='ensembl_annotation', doc_type="_doc", id=f"{data['species']}-{data['accession']}", body=data)
        print(f"{data['species']}-{res['result']}")
    except:
        print("Issue with loading annotation")


def empty_ensembl_annotation():
    try:
        es.delete_by_query(index='ensembl_annotation', body={"query": {"match_all": {}}})
    except:
        print("Issue with emptying ensembl_annotation index")


def main():
    print("koosum")
    print(ES_HOST)
    empty_ensembl_annotation()
    scrape_data('GENE-SWitCH')
    scrape_data('AQUA-FAANG')
    scrape_data('BovReg')


if __name__ == '__main__':
    main()
