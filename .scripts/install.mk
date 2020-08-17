# singer taps/targets handling

# where mara-singer is installed relative to the project root
mara-singer-package-dir ?= packages/mara-singer

# the directory of this Makefile in project
mara-singer-scripts-dir := $(dir $(lastword $(MAKEFILE_LIST)))

setup-mara-singer:
	make .copy-mara-app-scripts

# copy scripts from mara-singer package to project code
.copy-mara-singer-scripts:
	rsync --archive --recursive --itemize-changes  --delete $(mara-singer-package-dir)/.scripts/ $(mara-singer-scripts-dir)

# install singer packages from requirements.singer.txt
install-singer-packages:
	source .venv/bin/activate; .scripts/mara-singer/singer-cli.sh install -r singer-requirements.txt
