from extr_dgbr import DGBRXtractor


class ExtractCNAC:
    def __init__(self):
        self.target_url = "https://dados.gov.br/dataset/casos-nacionais"
        self.extensions = [".csv"]

    def download(self):
        xtr = DGBRXtractor("CasosNacionais")
        xtr.get_files_by_ds_url(self.target_url, ext=self.extensions)
