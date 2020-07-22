#!/bin/bash

export PALETTE_RESET='\e[0m'
export PALETTE_INFO='\e[33m' # brown color
export PALETTE_ERROR='\e[31m' # red color

trim()
{
    local trimmed="$1"

    # Strip leading space.
    trimmed="${trimmed## }"
    # Strip trailing space.
    trimmed="${trimmed%% }"

    echo "$trimmed"
}

if [[ $1 == 'install' ]]; then

	## install package

	if [[ $2 == '-r' ]] || [[ $2 == '--requirement' ]]; then

		## install from requirements file

		if [[ $3 == '' ]] || [[ -f "$3" ]]; then

			while IFS= read -r line
			do
				# cut comments from the line (only when not is git+ command)
				if ! [[ $line = '-e git+'* ]]; then
					line=$(echo "$line" | cut -f1 -d"#")
				fi
				# and trim whitespaces
				line=$(trim "$line")

				if [[ $line != '' ]]; then

					# get package name
					if [[ $line = '-e git+'* ]]; then
						PACKAGE_NAME=$(cut -d '=' -f2 <<< "$line")
					else
						PACKAGE_NAME="$line"

						# remove version info from package name
						PACKAGE_NAME=$(cut -d '~' -f1 <<< "$PACKAGE_NAME")
						PACKAGE_NAME=$(cut -d '!' -f1 <<< "$PACKAGE_NAME")
						PACKAGE_NAME=$(cut -d '>' -f1 <<< "$PACKAGE_NAME")
						PACKAGE_NAME=$(cut -d '<' -f1 <<< "$PACKAGE_NAME")
						PACKAGE_NAME=$(cut -d '=' -f1 <<< "$PACKAGE_NAME")
					fi

					# uninstall if it is already installed (we don't have a version handling yet!)
					if bash ./$0 list | grep "$PACKAGE_NAME" > /dev/null; then
						echo -e "${PALETTE_INFO}singer-cli.sh uninstall $PACKAGE_NAME${PALETTE_RESET}"
						bash ./$0 uninstall "$PACKAGE_NAME"
						RC=$?; [ $RC -ne 0 ] && exit $RC
					fi

					# run install
					echo -e "${PALETTE_INFO}singer-cli.sh install \"$line\"${PALETTE_RESET}"
					bash ./$0 install "$line"
					RC=$?; [ $RC -ne 0 ] && exit $RC
				fi
			done < $3

		else
			echo -e "${PALETTE_ERROR}requirements file '$3' not given or does not exist${PALETTE_RESET}"
			exit 1
		fi
	else

		## install from package name or git+ syntax

		PACKAGE_NAME="$2"

		if [[ $PACKAGE_NAME == '' ]]; then
			echo -e "${PALETTE_ERROR}Package name not given${PALETTE_RESET}"
			exit 1
		fi

		PIP_INSTALL_PARAM="$PACKAGE_NAME"
		if [[ $PACKAGE_NAME = '-e git+'* ]]; then

			PACKAGE_NAME=$(cut -d '=' -f2 <<< "$PIP_INSTALL_PARAM")

			if [[ $PACKAGE_NAME == '' ]] || [[ $PACKAGE_NAME == $PIP_INSTALL_PARAM ]]; then
				echo -e "${PALETTE_ERROR}When installing a package with git+, you have to add #egg= to specify the package name${PALETTE_RESET}"
				exit 1
			fi
		else
			# remove version info from package name
			PACKAGE_NAME=$(cut -d '~' -f1 <<< "$PACKAGE_NAME")
			PACKAGE_NAME=$(cut -d '!' -f1 <<< "$PACKAGE_NAME")
			PACKAGE_NAME=$(cut -d '>' -f1 <<< "$PACKAGE_NAME")
			PACKAGE_NAME=$(cut -d '<' -f1 <<< "$PACKAGE_NAME")
			PACKAGE_NAME=$(cut -d '=' -f1 <<< "$PACKAGE_NAME")
		fi

		CURRENT_ENV="$VIRTUAL_ENV"

		if [[ $CURRENT_ENV == '' ]]; then
			echo -e "${PALETTE_ERROR}You must run in an virtual environment to execute singer-cli${PALETTE_RESET}"
			exit 1
		fi

		PACKAGE_VENV="$CURRENT_ENV/../.singer-venv/$PACKAGE_NAME"

		python -m venv "$PACKAGE_VENV"
		source "$PACKAGE_VENV/bin/activate"
		pip install wheel
		RC=$?; [ $RC -ne 0 ] && exit $RC
		pip install $PIP_INSTALL_PARAM
		RC=$?; [ $RC -ne 0 ] && exit $RC
		source "$CURRENT_ENV/bin/activate"

		# create symbolic link

		SYMBOLIC_LINK_TARGET="../../.singer-venv/$PACKAGE_NAME/bin/$PACKAGE_NAME"
		SYMBOLIC_LINK_NAME="$CURRENT_ENV/bin/$PACKAGE_NAME"
		if [ -L "$SYMBOLIC_LINK_NAME" ]; then
			rm -f "$SYMBOLIC_LINK_NAME"
		fi

		ln -s "$SYMBOLIC_LINK_TARGET" "$SYMBOLIC_LINK_NAME"
		RC=$?; [ $RC -ne 0 ] && exit $RC

	fi

	# exist with OK
	exit 0

elif [[ $1 == 'uninstall' ]]; then

	## uninstall package

	if [[ $# -eq 1 ]]; then
		echo -e "${PALETTE_ERROR}Package name not given${PALETTE_RESET}"
		exit 1
	fi

	PACKAGE_NAME="$2"

	CURRENT_ENV="$VIRTUAL_ENV"

	if [[ $CURRENT_ENV == '' ]]; then
		echo -e "${PALETTE_ERROR}You must run in an virtual environment to execute singer-cli${PALETTE_RESET}"
		exit 1
	fi

	PACKAGE_VENV="$CURRENT_ENV/../.singer-venv/$PACKAGE_NAME"

	rm -f "$CURRENT_ENV/bin/$PACKAGE_NAME"
	rm -rf "$PACKAGE_VENV"

	exit 0

elif [[ $1 == 'list' ]]; then

	# list packages

	SEARCH_STRING='*'
	if [[ $# -gt 1 ]]; then
		SEARCH_STRING="$2"
	fi

	[ -d .singer-venv/ ] &&	find .singer-venv/$SEARCH_STRING -maxdepth 0 -mindepth 0 -type d -printf '%f\n'

else
	echo 'singer-cli.sh 0.3.0 [install|uninstall|list] [pip-package-name]'
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
	echo '  # call packages'
	echo '  tap-exchangeratesapi | target-csv'
	echo ''
	echo '  # uninstall packages'
	echo '  singer-cli.sh uninstall tap-exchangeratesapi'
	echo '  singer-cli.sh uninstall target-csv'
fi
