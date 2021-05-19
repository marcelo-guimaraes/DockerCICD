from extr_registro_civil import RegistroCivil
from datetime import date, timedelta, datetime
from utils.s3_writer_operator import HandlerS3Writer


class ExtractRCIV:
    def __init__(self, initial_date, final_dalte):
        self.states = [
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
        ]
        self.initial_date = initial_date
        self.final_date = final_dalte
        self.delta = final_dalte - initial_date

    def download(self):
        print("Downloading, please wait...")

        rcv = RegistroCivil()

        data = "data_coleta, data_registro, uf, municipio, obitos"
        extraction_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        for uf in self.states:
            for i in range(self.delta.days + 1):
                date = self.initial_date + timedelta(days=i)

                if date.day == 1:
                    register_date = date.strftime("%Y-%m")
                    req = rcv.obitos(
                        data_inicio=register_date, data_fim=register_date, uf=uf
                    )

                    for item in req["data"]:
                        city = item["name"]
                        deaths = str(item["total"])
                        data += (
                            "\n"
                            + extraction_date
                            + ","
                            + register_date
                            + ","
                            + uf
                            + ","
                            + city
                            + ","
                            + deaths
                        )

        s3_writer = HandlerS3Writer(
            extracted_file=data,
            extraction_name="obitos_geral.csv",
            extraction_source="ObitosRegistroCivil",
        )

        # with open('obitos-geral.csv', 'w') as f:
        #     f.write(data)
        #     f.close()

        print("Done.")
