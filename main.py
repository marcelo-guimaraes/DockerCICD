from extr_cnac import ExtractCNAC
from extr_rciv import ExtractRCIV
from extr_srag import ExtractSRAG

from datetime import datetime


def main():
    cnac = ExtractCNAC()
    cnac.download()

    rciv = ExtractRCIV(
        initial_date=datetime(2021, 1, 1), final_dalte=datetime(2021, 2, 28)
    )
    rciv.download()

    srag = ExtractSRAG()
    srag.download()


main()
