"""
Integration tests for the simeon CLI tool.
These can be quite expensive since they read from and write to files.
"""
import os
import subprocess as sb
import unittest


def _run_command(cmd):
    """
    Run the given command and return the generated Popen object
    """
    proc = sb.Popen(cmd.split(), stderr=sb.PIPE, stdout=sb.PIPE)
    proc.wait()
    return proc


class SimeonCLIIntegration(unittest.TestCase):
    """
    Test the simeon CLI command
    """
    def setUp(self):
        self.this_dir = os.path.dirname(os.path.abspath(__file__))
        self.log_file = os.path.join(
            self.this_dir, 'fixtures', 'file.log'
        )
        self.config_file = os.path.join(
            self.this_dir, 'fixture', 'config.ini'
        )
        self.good_subcommands = [
            'download', 'list', 'push', 'split'
        ]
        self.bad_subcommands = [
            'upload', 'inspect', 'run', 'stupid', 'spli', 'donwland',
            'psh', 'splt', 'lst', 'upoad', 'uload'
        ]
        self.global_options = [
            '--log-file {f}'.format(f=self.log_file),
            '--config-file {f}'.format(f=self.config_file),
            '--quiet', '--debug'
        ]
        self.proc = None

    def tearDown(self):
        if self.proc:
            if self.proc.stdout:
                self.proc.stdout.close()
            if self.proc.stderr:
                self.proc.stderr.close()
        for file_ in (self.log_file, self.config_file):
            try:
                os.remove(file_)
            except OSError:
                continue

    def test_good_subcommands(self):
        """
        Test that the subcommands have not been changed.
        """
        for subcommand in self.good_subcommands:
            cmd = 'simeon {sub} --help'.format(sub=subcommand)
            with self.subTest('Running {cmd}'.format(cmd=cmd)):
                self.proc = _run_command(cmd)
                self.assertEqual(self.proc.returncode, 0)
                self.tearDown()

    def test_bad_subcommands(self):
        """
        Test that mispelled subcommands should fail
        """
        for subcommand in self.bad_subcommands:
            cmd = 'simeon {sub} --help'.format(sub=subcommand)
            with self.subTest('Running {cmd}'.format(cmd=cmd)):
                self.proc = _run_command(cmd)
                self.assertNotEqual(self.proc.returncode, 0)
                self.tearDown()

    def test_global_options(self):
        """
        Test that the global options have not changed
        """
        for option in self.global_options:
            cmd = 'simeon {o} --help'.format(o=option)
            msg = 'Checking that global option {o} exists'.format(o=option)
            with self.subTest(msg):
                self.proc = _run_command(cmd)
                self.assertEqual(self.proc.returncode, 0)
                self.tearDown()


class SimeonGeoIPCLIIntegration(unittest.TestCase):
    """
    Test the simeon-geoip CLI tool
    """
    def setUp(self):
        self.this_dir = os.path.dirname(os.path.abspath(__file__))
        self.log_file = os.path.join(
            self.this_dir, 'fixtures', 'file.log'
        )
        self.config_file = os.path.join(
            self.this_dir, 'fixtures', 'config.ini'
        )
        self.un_file = os.path.join(
            self.this_dir, 'fixtures', 'un_data.csv'
        )
        self.output_file = os.path.join(
            self.this_dir, 'fixtures', 'geoip.json.gz'
        )
        self.global_options = [
            '--log-file {f}'.format(f=self.log_file),
            '--config-file {f}'.format(f=self.config_file),
            '--quiet',
        ]
        self.extract_good_options = [
            '--un-data {f}'.format(f=self.un_file),
            '--output {f}'.format(f=self.output_file),
            '--quiet', '--tracking-logs',
        ]
        self.merge_good_options = [
            '--geo-table dataset.table',
            '--project gcp-project-id',
            '--service-account-file saccount_file.json',
            '--column ip',
        ]
        self.proc = None

    def tearDown(self):
        if self.proc:
            streams = (self.proc.stdout, self.proc.stderr)
            for stream in streams:
                if stream:
                    stream.close()
        for file_ in (self.log_file, self.un_file, self.output_file):
            try:
                os.remove(file_)
            except OSError:
                continue

    def test_geoip_global_options(self):
        """
        Test that global options for simeon-geoip have not changed.
        """
        for option in self.global_options:
            cmd = 'simeon-geoip {o} --help'.format(o=option)
            msg = 'Checking that option {o} for simeon-geoip exists'.format(
                o=option
            )
            with self.subTest(msg):
                self.proc = _run_command(cmd)
                self.assertEqual(self.proc.returncode, 0)
                self.tearDown()

    def test_geoip_extract_options(self):
        """
        Test that options for simeon-geoip extract have not changed.
        """
        for option in self.global_options:
            cmd = 'simeon-geoip extract {o} --help'.format(o=option)
            msg = 'Checking that option {o} for extract exists'.format(
                o=option
            )
            with self.subTest(msg):
                self.proc = _run_command(cmd)
                self.assertEqual(self.proc.returncode, 0)
                self.tearDown()

    def test_geoip_merge_options(self):
        """
        Test that options for simeon-geoip merge have not changed.
        """
        for option in self.global_options:
            cmd = 'simeon-geoip merge {o} --help'.format(o=option)
            msg = 'Checking that option {o} for merge exists'.format(
                o=option
            )
            with self.subTest(msg):
                self.proc = _run_command(cmd)
                self.assertEqual(self.proc.returncode, 0)
                self.tearDown()


if __name__ == '__main__':
    unittest.main()
