#!/bin/bash

if [[ $1 == 'install' ]]; then

	## install package

	PACKAGE_NAME=$2
	
	if [[ $PACKAGE_NAME == '' ]]; then
		echo 'Package name not given'
		exit 1
	fi

	CURRENT_ENV=$VIRTUAL_ENV

	if [[ $CURRENT_ENV == '' ]]; then
		echo 'You must run in an virtual environment to execute singer-cli'
		exit 1
	fi

	PACKAGE_VENV=$CURRENT_ENV/../.singer-venv/$PACKAGE_NAME

	python -m venv $PACKAGE_VENV
	source $PACKAGE_VENV/bin/activate
	pip install wheel
	pip install $PACKAGE_NAME
	source $CURRENT_ENV/bin/activate
	ln -s ../../.singer-venv/$PACKAGE_NAME/bin/$PACKAGE_NAME $CURRENT_ENV/bin/$PACKAGE_NAME

	exit 0

elif [[ $1 == 'uninstall' ]]; then

	## uninstall package

	if [[ $# -eq 1 ]]; then
		echo 'Package name not given'
		exit 1
	fi

	PACKAGE_NAME=$2

	CURRENT_ENV=$VIRTUAL_ENV

	if [[ $CURRENT_ENV == '' ]]; then
		echo 'You must run in an virtual environment to execute singer-cli'
		exit 1
	fi

	PACKAGE_VENV=$CURRENT_ENV/../.singer-venv/$PACKAGE_NAME

	rm $CURRENT_ENV/bin/$PACKAGE_NAME
	rm -rf $PACKAGE_VENV

	exit 0

elif [[ $1 == 'list' ]]; then

	# list packages

	SEARCH_STRING='*'
	if [[ $# -gt 1 ]]; then
		SEARCH_STRING=$2
	fi

	find .singer-venv/$SEARCH_STRING -maxdepth 0 -mindepth 0 -type d -printf '%f\n'

else
	echo 'singer-cli.sh 0.2.0 [install|uninstall|list] [pip-package-name]'
	echo 'Usage: singer-cli.sh [command] [args]'
	echo ''
	echo 'singer-cli.sh is a simple package manager script for singer.io'
	echo 'tap/target pip packages'
	echo ''
	echo 'Commands:'
	echo '  install [package_name] - install a tap/target pip package in a isoladted environment'
	echo '  uninstall [package_name] - uninstall a tap/target pip package'
	echo '  list (optional_search_string) - list the installed packages'
	echo ''
	echo 'Sample usage:'
	echo '  # install packages'
	echo '  singer-cli.sh install tap-exchangeratesapi'
	echo '  singer-cli.sh install target-csv'
	echo ''
	echo '  # usae packages'
	echo '  tap-exchangeratesapi | target-csv'
	echo ''
	echo '  # uninstall packages'
	echo '  singer-cli.sh uninstall tap-exchangeratesapi'
	echo '  singer-cli.sh uninstall target-csv'
fi
