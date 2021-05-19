FROM selenium/standalone-chrome

# Diretório de trabalho
WORKDIR /app
# Copia arquivo requirements. Deve estar no mesmo path que o Dockerfile
COPY requirements.txt .

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

# Copia o arquivo entrypoint do repositorio para dentro do Docker
COPY entrypoint.sh /entrypoint.sh

# Executa o "starts up"
ENTRYPOINT ["/entrypoint.sh"]

# Inicia como root, upadate, intala pip3 a intala os requirements
USER root
RUN apt-get update
# RUN apt-get aws-cli
RUN apt-get install python3-pip -y
RUN pip3 install -r requirements.txt

# install selenium
RUN pip3 install selenium==3.8.0
RUN pip3 install html5lib lxml boto3

# Copia todos os arquivos que estão nesse diretorio para dentro da imagem
COPY ./ .

# Allow access to chromedriver (adding one that runs on Google Chrome v89)
RUN chmod +x drivers/chromedriver_linux_89
RUN chmod +x bash/configureAwsFiles.sh

# Gets passed AWS keys args into ~.aws/credentials folders
RUN ./bash/configureAwsFiles.sh ${AWS_ACCESS_KEY_ID} ${AWS_SECRET_ACCESS_KEY}

# Display port to avoid crash
ENV DISPLAY=:99

RUN echo

# Comando que será executado quando vc der o docker run 
CMD ["python3", "-u", "main.py"]

# Codigos Docker
#   Constroi o container:
#       docker build -t nome_imagem .
#   Roda o container
#       docker run nome_imagem
#   Entra dentro do terminal dentro do docker
#       docker run -it nome_imagem /bin/bash