# singer taps/targets handling

setup-mara-singer:
	make .copy-mara-app-scripts

# copy scripts from mara-singer package to project code
.copy-mara-singer-scripts:
	rsync --archive --recursive --itemize-changes  --delete packages/mara-app/.scripts/ .scripts/mara-app/

# install singer packages from requirements.singer.txt
install-singer-packages:
	source .venv/bin/activate; .scripts/mara-singer/singer-cli.sh install -r requirements.singer.txt
