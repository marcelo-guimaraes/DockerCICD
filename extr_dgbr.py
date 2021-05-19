import warnings
import os
import requests
import time
import sys
import json

from unidecode import unidecode

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

from utils.s3_writer_operator import HandlerS3Writer


class DGBRXtractor:
    def __init__(
        self,
        datasource,
    ):
        self.datasource = datasource
        pass

    def wb_start(self):
        # Ignorar avisos
        warnings.filterwarnings("ignore")

        # Configuração do webdriver
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_experimental_option(
            "excludeSwitches", ["enable-logging"]
        )

        # TODO This should not be here. Think of a way to better implement this variable
        if sys.platform == "linux" or sys.platform == "linux2":
            path_to_driver = "./drivers/chromedriver_linux89"
        if sys.platform == "darwin":
            path_to_driver = "./drivers/chromedriver_mac"
        if sys.platform == "win32":
            path_to_driver = "./drivers/chromedriver_windows.exe"

        self.browser = webdriver.Chrome(
            executable_path=path_to_driver,
            options=self.chrome_options,
        )

    """
        Este método foi desenvolvido para realizar o download de todos os arquivos do conjunto de dados a partir da sua url.
          url -> variável do tipo string | Contém a url da página do dataset que possui os arquivos a serem baixados.
          ext -> variável do tipo lista | Lista de strings com as extensões dos arquivos que deseja-se baixar. Por exemplo: ['.csv', '.xlsx', '.pdf']. O valor padrão é somente ['.csv'].
    """

    def get_files_by_ds_url(self, url, ext=[".csv"]):

        # Carregar configurações do WebBrowser
        self.wb_start()

        # Carregamento da página do dataset
        print("Carregando [{}]".format(url))

        try:
            init = time.time()
            self.browser.get(url)
            end = time.time()
            print("Página carregada com sucesso ({} s).".format(end - init))
        except:
            print("Ocorreu um erro ao carregar a página, tente novamente por favor.")
            self.browser.close()
            sys.exit()

        # Obter o nome do dataset a partir da url da página
        dataset_id = self.browser.current_url.split("/")[-1]

        # Selecionar os elementos HTML <li> que possuem a classe "resource-item" | file_elements é uma lista
        file_elements = self.browser.find_elements_by_css_selector("li.resource-item")

        # Iterar por todos os elementos armazenados em file_elements
        for element in file_elements:
            # Obter o formato do arquivo a ser baixado. Esse elemento se encontra no atributo "data-format" da tag <span> que fica dentro dos <li> que selecionamos anteriormente
            file_format = "." + element.find_element_by_css_selector(
                "span"
            ).get_attribute("data-format")

            # Transformar arquivos que são denotados como .zip+css ou .zip+xlsx e outros apenas em .zip
            if file_format.startswith(".zip"):
                file_format = ".zip"

            # Verificar se o formato de arquivo raspado está presente na lista de extensões que desejamos baixar
            if file_format in ext:
                # Gerar um nome para o arquivo a ser salvo. Aqui foi utilizado o atributo "title" das tags <a> que contém o link para o detalhamento do dataset, que possuem classe "heading"
                file_name = unidecode(
                    element.find_element_by_css_selector("a.heading")
                    .get_attribute("title")
                    .lower()
                    .strip()
                    .replace(" ", "-")
                    .replace("/", "-")
                )

                # Formatar o nome do arquivo para adicionar a extensão
                if not file_name.endswith(file_format):
                    file_name += file_format

                # Obter o link para download do arquivo. Ele se encontra no atributo "href" das tags <a> que possuem classe "resource-url-analytics"
                file_url = element.find_element_by_css_selector(
                    "a.resource-url-analytics"
                ).get_attribute("href")

                print(file_url)

                # Verificar se já existe uma pasta no diretório de execução com a identificação do dataset que pegamos lá em cima, caso não exista ela será criada
                if not os.path.exists(dataset_id):
                    os.makedirs(dataset_id)

                # Baixar o arquivo utilizando a biblioteca requests
                downloadable_url = file_url.split("=")[1]
                print("Downloading {}".format(file_name))
                req = requests.get(downloadable_url)

                # Definir o local e o nome que o arquivo será salvo
                file_path = dataset_id + "/" + file_name

                # Gravar o arquivo local
                # with open(file_path, mode="wb") as f:
                #   f.write(req.content)
                #   f.close()

                # Grava no S3
                s3_writer = HandlerS3Writer(
                    extracted_file=req.content,
                    extraction_name=file_name,
                    extraction_source=self.datasource,
                )

        # Fechar o browser
        self.browser.close()

    """
        Este método verifica se existe algum tipo de paginação na página atual.
    """

    def have_pagination(self):
        try:
            num_pages = self.browser.find_elements_by_css_selector("div.pagination li")[
                -2
            ].text
            return num_pages
        except:
            return 1

    """
        Este método cria um objeto .json contendo todas as organizações presentes no DADOS.GOV.BR,
        bem como seus URLS.
    """

    def update_organization_dictionary(self):

        print("Atualizando dicionário de organizações do DADOS.GOV.BR")

        # Carregar configurações do WebBrowser
        self.wb_start()

        # Carregar a página inicial das organizações
        url = "https://dados.gov.br/organization?q=&sort=&page="

        try:
            init = time.time()
            self.browser.get(url + "1")
            end = time.time()
            # print('Página carregada com sucesso ({} s).'.format(end-init))
        except:
            print(
                "Ocorreu um erro ao carregar a página para obter elementos de paginação, tente novamente por favor."
            )
            self.browser.close()
            sys.exit()

        # Verificar paginação
        pages = int(self.have_pagination())

        # Variável que irá receber o dicionário
        organizations = {}

        # Criar looping por todas as páginas
        for page in range(1, pages + 1):

            # Carregar a página
            try:
                init = time.time()
                completed_url = url + str(page)
                self.browser.get(completed_url)
                end = time.time()
                print(
                    "Página [{}] carregada com sucesso ({} s).".format(
                        completed_url, end - init
                    )
                )
            except:
                print(
                    "Ocorreu um erro ao carregar a página, tente novamente por favor."
                )
                self.browser.close()
                sys.exit()

            # Coletar as informações SIGLA, NOME e URL
            cards = self.browser.find_elements_by_css_selector("li.media-item a")

            for card in cards:
                org_name = card.get_attribute("title")
                org_initials = org_name.split(" ")[-1]
                org_url = card.get_attribute("href")

                organizations[org_initials] = {"name": org_name, "url": org_url}

        with open("org_dictionary.json", "wb") as f:
            f.write(json.dumps(organizations))
            f.close()

        print("Atualização realizada com êxito.")

    """
        Este método tem como finalidade recolher todos os links de datasets de uma organização,
        por meio do link da página desta organização no DADOS.GOV.BR.
          org_url -> variável do tipo string | URL da página da organização no DADOS.GOV.BR
    """

    def get_ds_urls_by_organization_url(self, org_url):

        print("Carregando URLs dos conjuntos de dados...")

        # Carregar as configurações do WebBrowser
        self.wb_start()

        # Carregar a página
        self.browser.get(org_url)

        # Verificar paginação
        pages = int(self.have_pagination())

        # Variável que irá armazenar os links
        datasets = []

        for page in range(1, pages + 1):

            # Carregar a página
            url = org_url + "?page=" + str(page)
            # print('carregando... ' + url)
            self.browser.get(url)

            # Obter os URLs do dataset
            ds_url_list = self.browser.find_elements_by_css_selector(
                "h3.dataset-heading a"
            )
            # print(ds_url_list)

            # Adicionando os links na lista
            for url in ds_url_list:
                datasets.append(url.get_attribute("href"))

        print("Datasets carregados com sucesso.")

        return datasets
