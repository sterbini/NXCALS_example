wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
bash Miniforge3-Linux-x86_64.sh -b  -p ./miniforge -f
source miniforge/bin/activate
python3 -m venv ./venv
source ./venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install git+https://gitlab.cern.ch/acc-co/devops/python/acc-py-pip-config.git
python -m pip install nxcals
python -m pip install -e nx2pd