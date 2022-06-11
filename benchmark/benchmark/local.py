import subprocess
from math import ceil
from os.path import basename, splitext, join
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, ConfigError, CarrierKey, \
    print_carriers
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 9000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Clean, download and compile carrier
            cmd = CommandMaker.clean_carrier().split()
            subprocess.run(cmd, check=True)
            # cmd = CommandMaker.download_carrier().split()
            cmd = f"cp -r /home/dmv18/epfl/project/carrier /home/dmv18/epfl/project/asonnino/hotstuff/".split() # TODO
            subprocess.run(cmd, check=True, cwd='..')
            cmd = CommandMaker.compile_carrier().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.carrier_path())

            # TODO add alias for carrier also (stage 2)
            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # TODO begin
            # Generate carrier key files
            carriers = []
            carrier_key_files = [PathMaker.carrier_key_file(i) for i in range(nodes)]
            for i in range(nodes):
                filename = carrier_key_files[i]
                address = committee.carrier2carrier[i]
                cmd = CommandMaker.generate_carrier_key(filename).split()
                subprocess.run(cmd, check=True)
                keypair = CarrierKey.from_file(filename)
                c = {"id": str(i), "address": str(address), "pk": keypair.pk}
                carriers.append(c)

            print_carriers("../carrier/.carriers.json", carriers)

            # TODO end
            # Do not boot faulty nodes.
            nodes = nodes - self.faults

            for i in range(nodes):
                log_file = PathMaker.carrier_log_file(i)
                client2carrier = committee.client2carrier[i]
                carrier2carrier = committee.carrier2carrier[i]
                front = committee.front[i]
                keypair = carrier_key_files[i]
                cmd = CommandMaker.run_carrier(client2carrier, carrier2carrier, front, keypair)
                self._background_run(cmd, log_file)

            # Run the clients (they will wait for the nodes to be ready).
            addresses = committee.client2carrier
            rate_share = ceil(rate / nodes)
            timeout = self.node_parameters.timeout_delay
            client_logs = [PathMaker.client_log_file(i) for i in range(nodes)]
            for addr, log_file in zip(addresses, client_logs):
                cmd = CommandMaker.run_client(
                    addr,
                    self.tx_size,
                    rate_share,
                    timeout
                )
                self._background_run(cmd, log_file)

            # Run the nodes.
            dbs = [PathMaker.db_path(i) for i in range(nodes)]
            node_logs = [PathMaker.node_log_file(i) for i in range(nodes)]
            for key_file, db, log_file in zip(key_files, dbs, node_logs):
                cmd = CommandMaker.run_node(
                    key_file,
                    PathMaker.committee_file(),
                    db,
                    PathMaker.parameters_file(),
                    debug=debug
                )
                self._background_run(cmd, log_file)

            # Wait for the nodes to synchronize
            Print.info('Waiting for the nodes to synchronize...')
            sleep(2 * self.node_parameters.timeout_delay / 1000)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process('./logs', faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
