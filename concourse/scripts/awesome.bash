#!/bin/bash

set -e -u -o pipefail
set -x

NCPU=$(getconf _NPROCESSORS_CONF)

# be a good neighbor: use all cores when possible, but back off if the worker is loaded
make_install() {
	make -s -j ${NCPU} -l $((2 * NCPU)) install "$@"
}

set_up_ccache_env() {
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
	make_install
}

fetch_and_build_xerces_c() {
	git clone https://github.com/greenplum-db/gp-xerces /tmp/xerces
	(
		set -e
		cd /tmp/xerces
		./configure --disable-network
	)
	make_install -C /tmp/xerces
}

fetch_and_build_orca() {
	fetch_and_build_xerces_c
	local orca_code_url
	orca_code_url=$(
		sed -E -n -e '/gporca/s,.*https://github.com/greenplum-db/gporca/releases/download/v(([[:digit:]]|\.)+)/bin_orca_centos5_release.tar.gz.*,https://github.com/greenplum-db/gporca/archive/v\1.tar.gz,p' <gpAux/releng/releng.mk
	)
	mkdir /tmp/orca
	wget -O - "${orca_code_url}" | tar zx --strip-components=1 -C /tmp/orca

	cmake -GNinja -H/tmp/orca -B/tmp/orca/build
	ninja install -C /tmp/orca/build
}

_main() {
	set_up_ccache_env

	fetch_and_build_orca

	configure

	time compile
}

_main "$@"
