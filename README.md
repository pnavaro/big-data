# Parallel Computing with Python

Notebooks for [Master for Smart Data Science ENSAI Rennes](https://ensai.fr/en/2-cursus/msc-statistics-for-smart-data/)

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/pnavaro/big-data/master)
[![JupyterBook](https://github.com/pnavaro/big-data//workflows/book/badge.svg)](https://github.com/pnavaro/big-data/actions/)

- [Website](https://pnavaro.github.io/big-data) generated with [jupyterbook](https://jupyterbook.org).
- [Website](https://pnavaro.github.io/big-data/quarto/intro.html) generated with [quarto](https://quarto.org).

The content of these notebooks are made thanks to these [references](https://pnavaro.github.io/big-data/intro.html).

## Run Jupyter notebooks with docker

### Get docker app

 - [Mac](https://www.docker.com/docker-mac)
 - [Windows](https://www.docker.com/docker-windows)
 - [Linux](https://runnable.com/docker/install-docker-on-linux)

You can run these notebooks with Docker. The following command starts a container with the Notebook 
server listening for HTTP connections on port 8888 and 4040 without authentication configured.

```
git clone https://github.com/pnavaro/big-data.git
docker run --rm -v $PWD/big-data:/home/jovyan/ -p 8888:8888 -p 4040:4040 pnavaro/big-data
```

<a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc/4.0/">Creative Commons Attribution-NonCommercial 4.0 International License</a>.
