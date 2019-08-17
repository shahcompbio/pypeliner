
yum install gcc-c++ git wget -y
conda config --set always_yes true
conda config --add channels 'bioconda'
conda install conda-build anaconda-client
conda build conda/pypeliner
anaconda -t $CONDA_UPLOAD_TOKEN upload /usr/local/conda-bld/linux-64/pypeliner-*.tar.bz2

