"""
Integration tests for the simeon CLI tool
"""
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
        self.good_subcommands = [
            'download', 'list', 'push', 'split'
        ]
        self.bad_subcommands = [
            'upload', 'inspect', 'run', 'stupid', 'spli', 'donwland',
            'psh', 'splt', 'lst', 'upoad', 'uload'
        ]
        self.global_options = [
            '--log-file file.log',
            '--config-file config.ini',
            '--quiet', '--debug'
        ]
        self.proc = None

    def tearDown(self):
        if self.proc:
            if self.proc.stdout:
                self.proc.stdout.close()
            if self.proc.stderr:
                self.proc.stderr.close()

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


if __name__ == '__main__':
    unittest.main()
