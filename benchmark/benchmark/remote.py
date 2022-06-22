import os

from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from os.path import basename, splitext
from time import sleep
from math import ceil
from os.path import join
import subprocess
from multiprocessing import Pool
import tqdm

from benchmark.config import Committee, Key, NodeParameters, BenchParameters,\
 CarrierParameters, ConfigError, Carrier
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker

from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    def __init__(self, ctx):
        self.manager = InstanceManager.make()
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    @staticmethod
    def _check_stderr(output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info('Installing rust, go and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',

            # The following dependencies prevent the error: [error: linker `cc` not found].
            'sudo apt-get -y install build-essential',
            'sudo apt-get -y install cmake',

            # Install rust (non-interactive).
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',

            # This is missing from the Rocksdb installer (needed for Rocksdb).
            'sudo apt-get install -y clang',
            
            # Install go
            # Annoyingly the command will error if go is already installed, hence || :
            # This way we won't know if there is an actual error during the installation
            'wget -q -O - https://git.io/vQhTU | bash || :',
            # 'sudo apt-get install golang -y',
            'source $HOME/.bashrc',

            # Clone the HotStuff repo.
            f'(git clone {self.settings.hs_repo_url} || (cd {self.settings.hs_repo_name} ; git pull))',
            # Clone the Carrier repo.
            f'(git clone {self.settings.carrier_repo_url} || (cd {self.settings.carrier_repo_name} ; git pull))'
        ]
        hosts = self.manager.hosts(flat=True)
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
            Print.heading(f'Initialized testbed of {len(hosts)} nodes')
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_logs() if delete_logs else 'true'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)']
        try:
            g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
            g.run(' && '.join(cmd), hide=True)
        except GroupException as e:
            raise BenchError('Failed to kill nodes', FabricError(e))

    def _select_hosts(self, bench_parameters):
        nodes = max(bench_parameters.nodes)

        # Ensure there are enough hosts.
        hosts = self.manager.hosts()
        if sum(len(x) for x in hosts.values()) < nodes:
            return []

        # Select the hosts in different data centers.
        ordered = zip(*hosts.values())
        ordered = [x for y in ordered for x in y]
        return ordered[:nodes]

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs=self.connect)
        output = c.run(cmd, hide=True)
        Bench._check_stderr(output)


    @staticmethod
    def _run_in_background(host, command, log_file, pkey):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        c = Connection(host, user='ubuntu', connect_kwargs={"pkey": pkey})
        output = c.run(cmd, hide=True)
        Bench._check_stderr(output)

    def _update(self, hosts):
        # TODO 1 add carrier
        Print.info(
            f'Updating {len(hosts)} nodes (branch "{self.settings.hs_branch}")...'
        )
        cmd = [
            f'(cd {self.settings.hs_repo_name} && git fetch -f)',
            f'(cd {self.settings.hs_repo_name} && git checkout -f {self.settings.hs_branch})',
            f'(cd {self.settings.hs_repo_name} && git pull -f)',
            f'(cd {self.settings.carrier_repo_name} && git fetch -f)',
            f'(cd {self.settings.carrier_repo_name} && git checkout -f {self.settings.carrier_branch})',
            f'(cd {self.settings.carrier_repo_name} && git pull -f)',
            'source $HOME/.cargo/env',
            # 'source $HOME/.bashrc', # It seems that this does not work as intended
            'export GOROOT=/home/ubuntu/.go',
            'export PATH=$GOROOT/bin:$PATH',
            'export GOPATH=/home/ubuntu/go',
            'export PATH=$GOPATH/bin:$PATH',
            f'(cd {self.settings.hs_repo_name}/node && {CommandMaker.compile()})',
            CommandMaker.alias_binaries(
                f'./{self.settings.hs_repo_name}/target/release/'
            ),
            f'(cd {self.settings.carrier_repo_name} && {CommandMaker.compile_carrier()})',
            CommandMaker.alias_carrier(
                f'./{self.settings.carrier_repo_name}'
            ),
        ]
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters, bench_parameters,
                carrier_parameters):
        # TODO 2. Generate and upload carrier configs
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        if carrier_parameters.enable:
            # Clean, download and compile carrier if enabled
            cmd = CommandMaker.clean_carrier().split()
            subprocess.run(cmd, check=True)
            # cmd = CommandMaker.download_carrier().split()
            cmd = f"cp -r /home/dmv18/epfl/project/carrier /home/dmv18/epfl/project/asonnino/hotstuff/".split()  # TODO
            subprocess.run(cmd, check=True, cwd='..')
            cmd = CommandMaker.compile_carrier().split()
            go_env = os.environ.copy()
            go_env["PATH"] = "/usr/local/go/bin:" + go_env["PATH"]
            subprocess.run(cmd, check=True, env=go_env,
                           cwd=PathMaker.carrier_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]
        consensus_addr = [f'{x}:{self.settings.consensus_port}' for x in hosts]
        front_addr = [f'{x}:{self.settings.front_port}' for x in hosts]
        mempool_addr = [f'{x}:{self.settings.mempool_port}' for x in hosts]
        committee = Committee(names, consensus_addr, front_addr, mempool_addr)
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        # Generate carrier configuration files
        if carrier_parameters.enable:
            hosts_file_data = {
                "hosts": hosts,
                "fronts": front_addr,
                "settings": {
                    "tsx-size": bench_parameters.tx_size,
                    "rate": bench_parameters.rate[0],
                    "nodes": len(bench_parameters.nodes),

                    "decision-port": self.settings.decision_port,
                    "carrier-port": self.settings.carrier_port,
                    "client-port": self.settings.client_port,

                    "mempool-threshold": carrier_parameters.init_threshold,
                    "forward-mode": 1 if carrier_parameters.forward_mode else 0,
                    "log-level": "info",

                    "carrier-conn-retry-delay":
                        carrier_parameters.carrier_conn_retry_delay,
                    "carrier-conn-max-retry":
                        carrier_parameters.carrier_conn_max_retry,
                    "node-conn-retry-delay":
                        carrier_parameters.node_conn_retry_delay,
                    "node-conn-max-retry":
                        carrier_parameters.node_conn_max_retry,

                    "local-base-port": carrier_parameters.local_base_port,
                    "local-front-port": carrier_parameters.local_front_port,
                }
            }
            # Dump data to .hosts.json
            Carrier.write_hosts_file(hosts_file_data)

            # Generate all the config files
            cmd = CommandMaker.generate_carrier_configs(".").split()
            subprocess.run(cmd, check=True)

        # Cleanup all nodes.
        cmd = f'{CommandMaker.cleanup()} || true'
        g = Group(*hosts, user='ubuntu', connect_kwargs=self.connect)
        g.run(cmd, hide=True)

        self.send_config_paralell(hosts, carrier_parameters.enable)
        # self.send_config_sequential(hosts)
        return committee

    def send_config_paralell(self, hosts, enable_carrier):
        tmp = []
        for i in range(0, len(hosts)):
            tmp += [(i, hosts[i], self.manager.settings.key_path,
                     enable_carrier)]
        try:
            with Pool() as p:
                for _ in tqdm.tqdm(
                        p.imap(Bench.send_host_config, tmp),
                        desc="Uploading config files",
                        bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                        total=len(tmp)):
                    pass
        except (ValueError, IndexError) as e:
            raise BenchError(f'Failed to upload configs: {e}')

    @staticmethod
    def send_host_config(tmp):
        (i, host, key_path, enable_carrier) = tmp
        pkey = RSAKey.from_private_key_file(key_path)
        c = Connection(host, user='ubuntu', connect_kwargs={"pkey": pkey})
        c.put(PathMaker.committee_file(), '.')
        c.put(PathMaker.key_file(i), '.')
        c.put(PathMaker.parameters_file(), '.')

        if enable_carrier:
            c.put(PathMaker.carrier_config_file(i), '.')

    def _run_single(self, hosts, rate, bench_parameters, node_parameters,
                    carrier_parameters, debug=False):
        Print.info('Booting testbed...')

        # Kill any potentially unfinished run and delete logs.
        self.kill(hosts=hosts, delete_logs=True)

        # Run the clients (they will wait for the nodes to be ready).
        # Filter all faulty nodes from the client addresses (or they will wait
        # for the faulty nodes to be online).
        committee = Committee.load(PathMaker.committee_file())
        clients = [f'{x}:{self.settings.client_port if carrier_parameters.enable else self.settings.front_port}' for x in hosts]  # Carrier client interfaces
        rate_share = ceil(rate / committee.size())  # Take faults into account.
        timeout = node_parameters.timeout_delay
        client_logs = [PathMaker.client_log_file(i) for i in range(len(hosts))]

        params = zip(hosts, clients, client_logs)
        indexed = zip(range(0, len(hosts)), params)
        tmp = [(i, self.manager.settings.key_path,
                rate_share, timeout, clients,
                bench_parameters, param) for i, param in indexed]
        try:
            with Pool() as p:
                for _ in tqdm.tqdm(
                        p.imap(Bench.boot_client, tmp),
                        desc="Booting clients",
                        bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                        total=len(tmp)):
                    pass
        except (ValueError, IndexError) as e:
            raise BenchError(f'Failed to boot clients: {e}')

        if carrier_parameters.enable:
            carrier_configs = [PathMaker.carrier_config_file(i) for i in range(len(hosts))]
            carrier_logs = [PathMaker.carrier_log_file(i) for i in range(len(hosts))]
            params = zip(hosts, carrier_configs, carrier_logs)
            indexed = zip(range(0, len(hosts)), params)
            tmp = [(i, self.manager.settings.key_path, param) for i, param in indexed]
            try:
                with Pool() as p:
                    for _ in tqdm.tqdm(
                            p.imap(Bench.boot_carrier, tmp),
                            desc="Booting carriers",
                            bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                            total=len(tmp)):
                        pass
            except (ValueError, IndexError) as e:
                raise BenchError(f'Failed to boot carriers: {e}')

        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        dbs = [PathMaker.db_path(i) for i in range(len(hosts))]
        node_logs = [PathMaker.node_log_file(i) for i in range(len(hosts))]
        decisions = [f'{x}:{self.settings.decision_port}' for x in hosts]

        params = zip(hosts, key_files, dbs, decisions, node_logs)
        indexed = zip(range(0, len(hosts)), params)
        tmp = [(i, self.manager.settings.key_path,
                debug, param) for i, param in indexed]
        try:
            with Pool() as p:
                for _ in tqdm.tqdm(
                        p.imap(Bench.boot_node, tmp),
                        desc="Booting nodes",
                        bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                        total=len(tmp)):
                    pass
        except (ValueError, IndexError) as e:
            raise BenchError(f'Failed to boot nodes: {e}')

        # Wait for the nodes to synchronize
        Print.info('Waiting for the nodes to synchronize...')
        sleep(2 * node_parameters.timeout_delay / 1000)

        # Wait for all transactions to be processed.
        duration = bench_parameters.duration
        for _ in progress_bar(range(20), prefix=f'Running benchmark ({duration} sec):'):
            sleep(ceil(duration / 20))

        sleep(2) # Make sure the clients have time to stop on their own
        self.kill(hosts=hosts, delete_logs=False)

    @staticmethod
    def boot_client(tmp):
        (i, key_path,
         rate_share, timeout, clients,
         bench_parameters, params) = tmp
        (host, client, log_file) = params

        pkey = RSAKey.from_private_key_file(key_path)

        cmd = CommandMaker.run_client(
            client,
            bench_parameters.tx_size,
            rate_share,
            timeout,
            nodes=clients,
        )
        Bench._run_in_background(host, cmd, log_file, pkey)

    @staticmethod
    def boot_carrier(tmp):
        (i, key_path, params) = tmp
        (host, config_file, log_file) = params

        pkey = RSAKey.from_private_key_file(key_path)

        cmd = CommandMaker.run_carrier_remote(config_file)
        Bench._run_in_background(host, cmd, log_file, pkey)

    @staticmethod
    def boot_node(tmp):
        (i, key_path, debug, params) = tmp
        (host, key_file, db, decision, log_file) = params

        pkey = RSAKey.from_private_key_file(key_path)

        cmd = CommandMaker.run_node(
            key_file,
            PathMaker.committee_file(),
            db,
            PathMaker.parameters_file(),
            decision,
            debug=debug
        )
        Bench._run_in_background(host, cmd, log_file, pkey)

    def _logs(self, hosts, faults, enable_carrier, carrier_init_threshold, tx_size):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Download log files.
        self._get_logs_parallel(hosts, enable_carrier)

        return LogParser.process(PathMaker.logs_path(), faults, enable_carrier, carrier_init_threshold, tx_size)

    def _get_logs_parallel(self, hosts, enable_carrier):

        tmp = []
        for i in range(0, len(hosts)):
            tmp += [(i, hosts[i], self.manager.settings.key_path,
                     enable_carrier)]
        try:
            with Pool() as p:
                for _ in tqdm.tqdm(
                        p.imap(Bench.get_host_logs, tmp),
                        desc="Downloading logs",
                        bar_format='{l_bar}{bar:20}{r_bar}{bar:-10b}',
                        total=len(tmp)):
                    pass
        except (ValueError, IndexError) as e:
            raise BenchError(f'Failed to download logs: {e}')

    @staticmethod
    def get_host_logs(tmp):
        (i, host, key_path, enable_carrier) = tmp
        pkey = RSAKey.from_private_key_file(key_path)
        c = Connection(host, user='ubuntu', connect_kwargs={"pkey": pkey})
        c.get(PathMaker.node_log_file(i), local=PathMaker.node_log_file(i))
        c.get(PathMaker.client_log_file(i), local=PathMaker.client_log_file(i))
        if enable_carrier:
            c.get(PathMaker.carrier_log_file(i), local=PathMaker.carrier_log_file(i))

        return 0

    def run(self, bench_parameters_dict, node_parameters_dict,
            carrier_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
            carrier_parameters = CarrierParameters(carrier_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        if carrier_parameters.enable:
            Print.info('Carrier enabled')
        else:
            Print.info('Carrier disabled')
        # Update nodes.
        try:
            self._update(selected_hosts)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        cmd = 'ulimit -n 1048575'
        subprocess.run([cmd], shell=True)

        # Run benchmarks.
        for n in bench_parameters.nodes:
            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')
                hosts = selected_hosts[:n]

                # Upload all configuration files.
                try:
                    self._config(hosts, node_parameters, bench_parameters,
                                 carrier_parameters)
                except (subprocess.SubprocessError, GroupException) as e:
                    e = FabricError(e) if isinstance(e, GroupException) else e
                    Print.error(BenchError('Failed to configure nodes', e))
                    continue

                # Do not boot faulty nodes.
                faults = bench_parameters.faults
                hosts = hosts[:n-faults]

                # Run the benchmark.
                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        self._run_single(
                            hosts, r, bench_parameters, node_parameters, carrier_parameters, debug
                        )
                        self._logs(hosts, faults, carrier_parameters.enable, carrier_parameters.init_threshold, bench_parameters.tx_size).print(PathMaker.result_file(
                            faults, n, r, bench_parameters.tx_size,
                            carrier_parameters.enable
                        ))
                    except (subprocess.SubprocessError, GroupException, ParseError) as e:
                        self.kill(hosts=hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue
