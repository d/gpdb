#!/bin/bash

set -e -u -o pipefail
set -x

NCPU=$(getconf _NPROCESSORS_CONF)
readonly NCPU

: ${USER:=$(whoami)}

make_install() {
	sudo make -s install "$@"
}

# be a good neighbor: use all cores when possible, but back off if the worker is loaded
make_nicely() {
	make -s -j ${NCPU} -l $((2 * NCPU)) "$@"
}

set_up_ccache_env() {
	# FIXME: is this Debianism? Maybe tell configure (twice) with CC=
	if [ $(readlink -f $(type -p gcc)) != $(which ccache) ]; then
		if [ -d /usr/lib/ccache ]; then
			PATH=/usr/lib/ccache:$PATH
		fi
	fi
	: ${CCACHE_DIR:=$PWD/.ccache}
	export CCACHE_DIR
}

configure() {
	./configure --enable-depend --enable-debug --enable-cassert --with-libxml --with-zstd --with-python
}

compile() {
	make_nicely all
	make_install
}

fetch_and_build_xerces_c() {
	# TODO: orca publishing the corresponding xerces with each release
	git clone https://github.com/greenplum-db/gp-xerces /tmp/xerces
	(
		set -e
		cd /tmp/xerces
		./configure --disable-network
	)
	make_nicely -C /tmp/xerces
	make_install -C /tmp/xerces
}

fetch_and_build_orca() {
	fetch_and_build_xerces_c
	local orca_code_url
	orca_code_url=$(
		sed -E -n -e '/gporca/s,.*https://github.com/greenplum-db/gporca/releases/download/v(([[:digit:]]|\.)+)/bin_orca_centos5_release.tar.gz.*,https://github.com/greenplum-db/gporca/archive/v\1.tar.gz,p' <gpAux/releng/releng.mk
	)
	local ORCA_SRC ORCA_BUILD
	readonly ORCA_SRC=/tmp/orca
	readonly ORCA_BUILD=/tmp/orca/build
	mkdir "${ORCA_SRC}"
	wget -qO - "${orca_code_url}" | tar zx --strip-components=1 -C "${ORCA_SRC}"

	cmake -GNinja -H"${ORCA_SRC}" -B"${ORCA_BUILD}"
	ninja -C "${ORCA_BUILD}"
	sudo ninja install -C "${ORCA_BUILD}"
}

workaround_concourse_file_uids() {
	sudo chown -R "${USER}:${USER}" .
}

_main() {
	workaround_concourse_file_uids

	set_up_ccache_env

	fetch_and_build_orca

	configure

	time compile
}

_main "$@"
