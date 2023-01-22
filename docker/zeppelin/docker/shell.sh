sudo docker build -t mi_imagen .
sudo docker run -it --rm --name prueba mi_imagen

sudo docker run --rm -p 8080:8080 --name prueba mi_imagen
