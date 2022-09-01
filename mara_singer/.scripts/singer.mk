# singer taps/targets handling

setup-singer: .copy-mara-singer-scripts

# copy scripts from mara-singer package to project code
.copy-mara-singer-scripts: MODULE_LOCATION != .venv/bin/python -m pip show mara-singer | sed -n -e 's/Location: //p'
.copy-mara-singer-scripts:
	rsync --archive --recursive --itemize-changes  --delete $(MODULE_LOCATION)/mara_singer/.scripts/ .scripts/mara-singer/

# install singer packages from singer-requirements.txt.freeze
install-singer-packages:
	make .venv/bin/python
	source .venv/bin/activate; .scripts/mara-singer/singer-cli.sh install --requirement=singer-requirements.txt.freeze --src=./packages --upgrade --exists-action=w

# update packages from singer-requirements.txt and create singer-requirements.txt.freeze
update-singer-packages:
	make .venv/bin/python
	source .venv/bin/activate; PYTHONWARNINGS="ignore" .scripts/mara-singer/singer-cli.sh install --requirement=singer-requirements.txt --src=./packages --upgrade --exists-action=w

	# write freeze file
	source .venv/bin/activate; .scripts/mara-singer/singer-cli.sh freeze > singer-requirements.txt.freeze

.cleanup-singer:
	rm -rf .singer