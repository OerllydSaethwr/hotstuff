from json import load, JSONDecodeError


class SettingsError(Exception):
    pass


class Settings:
    def __init__(self, testbed, key_name, key_path, consensus_port, mempool_port, front_port,
                 client_port, carrier_port, decision_port,
                 hs_repo_name, hs_repo_url, hs_branch,
                 carrier_repo_name, carrier_repo_url, carrier_branch,
                 instance_type, aws_regions,
                 validity_duration
                 ):
        if isinstance(aws_regions, list):
            regions = aws_regions
        else:
            [aws_regions]

        inputs_str = [
            testbed, key_name, key_path, hs_repo_name, hs_repo_url, hs_branch, instance_type,
            carrier_repo_name, carrier_repo_url, carrier_branch
        ]
        inputs_str += regions
        inputs_int = [consensus_port, mempool_port, front_port, client_port, carrier_port, decision_port]
        ok = all(isinstance(x, str) for x in inputs_str)
        ok &= all(isinstance(x, int) for x in inputs_int)
        ok &= len(regions) > 0
        if not ok:
            raise SettingsError('Invalid settings types')

        self.testbed = testbed

        self.key_name = key_name
        self.key_path = key_path

        self.consensus_port = consensus_port
        self.mempool_port = mempool_port
        self.front_port = front_port

        self.client_port = client_port
        self.carrier_port = carrier_port
        self.decision_port = decision_port

        self.hs_repo_name = hs_repo_name
        self.hs_repo_url = hs_repo_url
        self.hs_branch = hs_branch

        self.carrier_repo_name = carrier_repo_name
        self.carrier_repo_url = carrier_repo_url
        self.carrier_branch = carrier_branch

        self.instance_type = instance_type
        self.aws_regions = regions

        self.validity_duration = validity_duration

    @classmethod
    def load(cls, filename):
        try:
            with open(filename, 'r') as f:
                data = load(f)

            return cls(
                data['testbed'],
                data['key']['name'],
                data['key']['path'],
                data['ports']['consensus'],
                data['ports']['mempool'],
                data['ports']['front'],
                data['ports']['client'],
                data['ports']['carrier'],
                data['ports']['decision'],
                data['hs_repo']['name'],
                data['hs_repo']['url'],
                data['hs_repo']['branch'],
                data['carrier_repo']['name'],
                data['carrier_repo']['url'],
                data['carrier_repo']['branch'],
                data['instances']['type'],
                data['instances']['regions'],
                data['validity']['duration'],
            )
        except (OSError, JSONDecodeError) as e:
            raise SettingsError(str(e))

        except KeyError as e:
            raise SettingsError(f'Malformed settings: missing key {e}')
