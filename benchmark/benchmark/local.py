import os
import subprocess
from math import ceil
from os.path import basename, splitext, join
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, LocalCommittee, NodeParameters, BenchParameters, CarrierParameters, ConfigError, CarrierKey, \
    print_carriers, Carrier

from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 9000

    def __init__(self, bench_parameters_dict, node_parameters_dict,
                 carrier_params_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
            self.carrier_parameters = CarrierParameters(carrier_params_dict)
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
        go_env = os.environ.copy()
        go_env["PATH"] = "/usr/local/go/bin:" + go_env["PATH"]
        enable_carrier = self.carrier_parameters.enable

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
            if enable_carrier:
                cmd = CommandMaker.clean_carrier().split()
                subprocess.run(cmd, check=True)
                # cmd = CommandMaker.download_carrier().split()
                cmd = f"cp -r /home/dmv18/epfl/project/carrier /home/dmv18/epfl/project/asonnino/hotstuff/".split() # TODO
                subprocess.run(cmd, check=True, cwd='..')


                cmd = CommandMaker.compile_carrier().split()
                subprocess.run(cmd, check=True, cwd=PathMaker.carrier_path(),
                               env=go_env)

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

            base_port = 6000
            ports_per_carrier = 3 # Leaving this in as this is not likely to change

            clients = [] if enable_carrier else committee.front
            decisions = [] if enable_carrier else ["127.0.0.1:"+str(base_port + i*ports_per_carrier + 1) for i in range(nodes)]

            if enable_carrier:
                for i in range(nodes):
                    decisions.append("127.0.0.1:"+str(base_port + i*ports_per_carrier + 1))
                    clients.append("127.0.0.1:"+str(base_port + i*ports_per_carrier + 2))

                hosts_file_data = {
                    "hosts": ["127.0.0.1"] * nodes,
                    "fronts": committee.front,
                    "settings": {
                        "tsx-size": self.bench_parameters.tx_size,
                        "rate": self.bench_parameters.rate[0],
                        "nodes": len(self.bench_parameters.nodes),

                        "decision-port": 0, # Unused in the local case
                        "carrier-port": 0,
                        "client-port": 0,

                        "mempool-threshold": self.carrier_parameters.init_threshold,
                        "forward-mode": 1 if self.carrier_parameters.forward_mode else 0,
                        "log-level": "info",

                        "carrier-conn-retry-delay":
                            self.carrier_parameters.carrier_conn_retry_delay,
                        "carrier-conn-max-retry":
                            self.carrier_parameters.carrier_conn_max_retry,
                        "node-conn-retry-delay":
                            self.carrier_parameters.node_conn_retry_delay,
                        "node-conn-max-retry":
                            self.carrier_parameters.node_conn_max_retry,

                        "local-base-port": self.carrier_parameters.local_base_port,
                        "local-front-port": self.carrier_parameters.local_front_port,
                    }
                }
                Carrier.write_hosts_file(hosts_file_data)

                cmd = CommandMaker.make_config_dir().split()
                subprocess.run(cmd, check=True)

                cmd = CommandMaker.generate_carrier_configs(PathMaker.carrier_config_path()).split()
                subprocess.run(cmd, check=True)

            # Do not boot faulty nodes.
            nodes = nodes - self.faults

            # Run carriers
            if enable_carrier:
                for i in range(nodes):
                    log_file = PathMaker.carrier_log_file(i)
                    cmd = CommandMaker.run_carrier(f'{PathMaker.carrier_config_path()}/.carrier-' + str(i) + '.json')
                    self._background_run(cmd, log_file)

            # Run the clients (they will wait for the nodes to be ready).
            rate_share = ceil(rate / nodes)
            timeout = self.node_parameters.timeout_delay
            client_logs = [PathMaker.client_log_file(i) for i in range(nodes)]
            for addr, log_file in zip(clients, client_logs):
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
            for key_file, db, log_file, decision in zip(key_files, dbs, node_logs, decisions):
                cmd = CommandMaker.run_node(
                    key_file,
                    PathMaker.committee_file(),
                    db,
                    PathMaker.parameters_file(),
                    decision,
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
            return LogParser.process('./logs', self.faults, enable_carrier, self.carrier_parameters.init_threshold, self.bench_parameters.tx_size)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
