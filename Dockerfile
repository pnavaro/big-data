FROM jupyter/pyspark-notebook

MAINTAINER Pierre Navaro <pierre.navaro@univ-rennes2.fr>

USER root

COPY . ${HOME}

RUN chown -R ${NB_USER} ${HOME}

USER $NB_USER

RUN conda install --yes --quiet -c conda-forge mamba && \
    mamba install --yes --quiet dask-labextension jupytext graphviz && \
    mamba env create --quiet -f environment.yml && \
    conda clean --all -f -y && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

RUN jupyter labextension install dask-labextension && \
    jupyter serverextension enable dask_labextension && \
    jupyter nbextension install --py jupytext --user && \
    jupyter lab build && \
    conda run -n big-data python -m ipykernel install --user --name big-data
