from json import dump
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; rm -r carrier-config ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def download_carrier():
        return 'git clone https://github.com/OerllydSaethwr/carrier.git'

    @staticmethod
    def compile_carrier():
        return f'go build cmd/cobra/carrier.go'

    @staticmethod
    def clean_carrier():
        return f'rm -rf {PathMaker.carrier_path()}'


    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node keys --filename {filename}'

    @staticmethod
    def run_node(keys, committee, store, parameters, decision, debug=False):
        assert isinstance(keys, str)
        assert isinstance(committee, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --keys {keys} --committee {committee} '
                f'--store {store} --parameters {parameters} --decision {decision}')

    @staticmethod
    def run_client(address, size, rate, timeout, nodes=[]):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return (f'./client {address} --size {size} '
                f'--rate {rate} --timeout {timeout} {nodes}')

    @staticmethod
    def run_carrier(configFile):
        assert isinstance(configFile, str)

        return f'{PathMaker.carrier_path()}/carrier {configFile}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'client')
        return f'rm node ; rm client ; ln -s {node} . ; ln -s {client} . '

    @staticmethod
    def alias_carrier(origin):
        assert isinstance(origin, str)
        carrier = join(origin, 'carrier')
        return f'rm -f carrier-exe ; ln -s {carrier} ./carrier-exe'

    @staticmethod
    def generate_carrier_configs():
        return f'{PathMaker.carrier_path()}/carrier generate config {PathMaker.hosts_file_path()} {PathMaker.config_path()}'

    @staticmethod
    def make_config_dir():
        return f'mkdir {PathMaker.config_path()}'

