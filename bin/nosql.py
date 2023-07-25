#! /usr/bin/env python3
#
# Copyright (c) 2018, 2023 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

#
# A simple interactive shell for an Oracle NoSQL Database instance. The instance
# can be the cloud service, an on-premise instance (via proxy), or a Cloudsim
# instance.
# Usage: nosql.py -s|--service <service> -e|--endpoint <endpoint> [-v]
#    [-c|--creds <credentials_file>] - cloud service credentials file
#    [-t|--tenant <tenant_id>] - cloud service tenant ocid
#    [-u|--user <user_id>] - cloud service tenancy user ocid
#    [-f|--fingerprint <fingerprint>] - cloud service fingerprint for user
#    [-k|--key <private-key>] - cloud service private key file path or string
#    [-p|--pass <pass_phrase for key or password>] - private key pass or user password
#
#  -v for verbose
#  service is one of <cloud | cloudsim | kvstore>
#  endpoint is a cloud region or url to a cloudsim or on-prem proxy, e.g.
#     -e us-ashburn-1
#     -e http://localhost:8080
#
# Requirements:
#  1. Python 3.6+
#  2. Python dependencies (install using pip or other mechanism):
#   o borneo
#    $ pip install borneo
#   o cmd2
#    $ pip install cmd2
#   o pyreadline or gnureadline (for cmd2)
#    $ pip install gnureadline
#  3. If running against the Cloud Simulator, it can be downloaded from
#  here:
#   http://www.oracle.com/technetwork/topics/cloud/downloads/index.html
#  The Cloud Simulator requires Java
#  4. If running against the Oracle NoSQL Database Cloud Service an account
#  must be used.
#
#
# Enhancement list:
#  o see cmd2 features and enable what's reasonable
#    https://github.com/python-cmd2/cmd2
#    https://cmd2.readthedocs.io/en/0.9.3/freefeatures.html
#  o add logging configuration for logger (it is turned off)
#  o add variables (set) along with a way to reference them
#    (does cmd2 do this - yes but using "py" command)
#  o queries with bind vars:
#    - store prepared queries by name, e.g.: set q1 as prepare ....
#    - allow setting of variables by prepared name + bind var name
#    - execute stored, prepared query by name
#    - would need commands to list, clear these things
#  o output to files/null
#  o load/run scripts (this "just works" I think)
#  o add op name to timer call?
#  o add on-prem secure config
#  o add flag for non-DEFAULT OCI profile in config
#  o add more commands and options to cover the complete API, including
#  some from the OCI SDK
#

import getopt
import json
import os
import os.path
import pprint
import shlex
import sys
import time
import traceback

import cmd2

from borneo import (
    AuthorizationProvider, GetRequest, GetTableRequest,
    IllegalArgumentException, ListTablesRequest, NoSQLHandle,
    NoSQLHandleConfig, PrepareRequest, PutOption, PutRequest,
    QueryRequest, TableLimits, TableRequest, TableUsageRequest)
from borneo.iam import SignatureProvider
from borneo.kv import StoreAccessTokenProvider

# if use of a local borneo (nosql-python-sdk) repository is desired,
# edit and uncomment this line
# sys.path.append('path-to-nosql-python-sdk/src')

cloudsim_id = 'cloudsim_user'


class CloudsimAuthorizationProvider(AuthorizationProvider):
    """
    Cloud Simulator Only.

    This class is used as an AuthorizationProvider when using the Cloud
    Simulator, which has no security configuration. It accepts a string
    tenant_id that is used as a simple namespace for tables.
    """

    def __init__(self, tenant_id):
        super(CloudsimAuthorizationProvider, self).__init__()
        self._tenant_id = tenant_id

    def close(self):
        pass

    def get_authorization_string(self, request=None):
        return 'Bearer ' + self._tenant_id


def get_handle(provider,
               endpoint,
               rate_limiting_enabled):
    """
    Returns a NoSQLHandle based on the provider and endpoint.
    """
    try:
        cfg = NoSQLHandleConfig(endpoint, provider)
        cfg.set_pool_connections(3)
        cfg.set_pool_maxsize(3)
        cfg.set_rate_limiting_enabled(rate_limiting_enabled)
        # no logging for now (TODO: add to command line and command)
        cfg.set_logger(None)
        return NoSQLHandle(cfg)
    except Exception as err:
        print(err)
        usage(2)


#
# create an AuthorizationProvider for the requested environment
#
#
def get_provider(service,
                 creds_file=None,
                 tenant_id=None,
                 user_id=None,
                 fingerprint=None,
                 private_key=None,
                 pass_phrase=None):
    """
    Returns a AuthorizationProvider based on the requested service type and
    optional parameters.Differences among the supported environments are
    encapsulated in this method.
    """
    try:
        if service == 'cloud':
            #
            # Get credentials using SignatureProvider.
            #
            if creds_file is not None:
                print("Using credentials from the DEFAULT profile in file {f}".
                      format(f=creds_file))
                return SignatureProvider(config_file=creds_file)

            elif tenant_id is not None:
                print('Using directly provided credentials')

                # if the key string points to a file try to open it and read
                # the contents and use that as the key
                if private_key is not None and os.path.isfile(private_key):
                    with open(private_key) as f:
                        private_key = f.read().rstrip("\n")

                #
                # Credentials are provided directly
                #
                return SignatureProvider(tenant_id=tenant_id,
                                         user_id=user_id,
                                         fingerprint=fingerprint,
                                         private_key=private_key,
                                         pass_phrase=pass_phrase)
            else:
                #
                # Credentials will come from the default config file.
                #
                print('Using credentials and DEFAULT profile from ' +
                      '~/.oci/config')
                return SignatureProvider()
        elif service == 'cloudsim':
            print('Using cloud simulator')
            return CloudsimAuthorizationProvider(cloudsim_id)

        elif service == 'kvstore':
            print('Using on-premise service')
            return StoreAccessTokenProvider()

        else:
            raise IllegalArgumentException('Unknown environment: ' + service)

    except Exception as err:
        print(err)
        usage(2)


class Timer:
    """
    A class to time operations
    """

    def __init__(self, shell):
        if shell.is_timer_on():
            self._start = time.perf_counter()
        else:
            self._start = None

    def stop(self):
        if self._start is not None:
            end = time.perf_counter()
            print(f"Operation took {end - self._start:0.4f} seconds")


#
# Main shell
#
# noinspection PyStatementEffect
class NoSQLShell(cmd2.Cmd):

    def __init__(self, service, provider, endpoint, compartment, verbose):
        self._service = service
        self._handle = None
        self._ddl_timeout_ms = 20000
        self._request_timeout_ms = 10000
        self._poll_ms = 1000
        self._last_output = None
        self._endpoint = endpoint
        self._verbose = verbose
        self._pretty = False
        self._json = False
        self._stats = None
        self._timer = False
        self._rate_limiting_enabled = True
        self._compartment = compartment
        # throughput
        self._tp = dict(readkb=0, writekb=0, read_units=0, write_units=0)
        self._pprint = pprint.PrettyPrinter(indent=2)
        self._handle = get_handle(provider,
                                  endpoint,
                                  self._rate_limiting_enabled)
        self.echo = False
        self.intro = ('Welcome to the NoSQL shell. '
                      'Type help or ? to list commands\n')
        self.prompt = '<nosql> '
        shortcuts = cmd2.DEFAULT_SHORTCUTS
        super().__init__(multiline_commands=['ddl', 'query', 'select'],
                         shortcuts=shortcuts, allow_cli_args=False)

    def is_timer_on(self):
        return self._timer

    # noinspection PyUnusedLocal
    def do_connect(self, arg):
        """Connect to NoSQL"""
        self._handle = get_handle(self._service,
                                  self._endpoint,
                                  self._rate_limiting_enabled)

    # noinspection PyUnusedLocal
    def do_disconnect(self, arg):
        """Disconnect from NoSQL"""
        self.close()
        self._handle = None

    # noinspection PyUnusedLocal
    def do_timer(self, arg):
        """Toggle a timer to display time associated with a command"""
        self._timer = not self._timer
        if self._timer:
            print("Timer is on")
        else:
            print("Timer is off")

    def do_timeout(self, arg):
        """Set or get the request timeout: timeout [timeout_ms]"""
        if arg != '':
            self._request_timeout_ms = arg
        print("Request timeout (ms) is {t}".format(t=self._request_timeout_ms))

    # noinspection PyUnusedLocal
    def do_pretty(self, arg):
        """Toggle pretty-printed output for values: pretty (see also: json)"""
        self._pretty = not self._pretty
        if self._pretty:
            print("Pretty printing is on")
        else:
            print("Pretty printing is off")

    # noinspection PyUnusedLocal
    def do_json(self, arg):
        """Toggle JSON vs dictionary output for values (see also: pretty)"""
        self._json = not self._json
        if self._json:
            print("JSON printing is on")
        else:
            print("JSON printing is off")

    def do_compartment(self, arg):
        """Set or show compartment (cloud only): compartment [ocid]"""
        if self._service != "cloud":
            print("compartment is invalid for non-cloud services")
        else:
            if arg != '':
                self._compartment = arg
            print("Using compartment: {c}".format(c=self._compartment))

    def do_show_tables(self, arg):
        """Show tables: show_tables ['table1[,tableN]*']"""
        if not self.ensure_handle():
            return False
        t = Timer(self)
        if arg != '':
            args = arg.replace(",", " ").split()
            for table in args:
                req = GetTableRequest().set_table_name(table)
                req.set_compartment(self._compartment)
                result = self._handle.get_table(req)
                print("")
                print(result)
        else:
            ltr = ListTablesRequest()
            ltr.set_compartment(self._compartment)
            result = self._handle.list_tables(ltr)
            for table in result.get_tables():
                print(table)
        t.stop()

    #
    # TODO: add optional start/end times for usage records
    #
    def do_table_usage(self, arg):
        """Table usasge: table_usage <table>"""
        if not self.ensure_handle():
            return False
        t = Timer(self)
        tu = TableUsageRequest().set_table_name(arg)
        tu.set_compartment(self._compartment)
        result = self._handle.get_table_usage(tu)
        print(result)
        t.stop()

    def do_ddl(self, arg):
        """Execute a DDL operation (terminate with ';' or blank line):
        ddl 'statement' ['read,write,size']
        """
        if not self.ensure_handle():
            return False

        t = Timer(self)
        arg = self.strip_semi(arg)
        args = shlex.split(arg)
        if len(args) > 2:
            print("Usage: ddl 'statement' ['read,write,size'] " +
                  "(both must be quoted)")
            return False
        print(args)
        statement = args[0]
        limits = None
        if len(args) > 1:
            limits = self.make_limits(args[1])
            if limits is None:
                return False
        creq = TableRequest().set_statement(statement)
        if limits is not None:
            creq.set_table_limits(limits)
        creq.set_compartment(self._compartment)
        result = self._handle.do_table_request(creq,
                                               self._ddl_timeout_ms,
                                               self._poll_ms)
        print(result)
        t.stop()
        return False

    def do_change_limits(self, arg):
        """Change limits on a table: change_limits <table_name> 'read,write,size'"""
        if not self.ensure_handle():
            return False
        t = Timer(self)
        args = shlex.split(arg)
        if len(args) != 2:
            print("Usage: change_limits <table_name> 'read,write,size'")
            return False
        limits = self.make_limits(args[1])
        creq = TableRequest().set_table_name(args[0]).set_table_limits(limits)
        creq.set_compartment(self._compartment)
        result = self._handle.do_table_request(creq,
                                               self._ddl_timeout_ms,
                                               self._poll_ms)
        print(result)
        t.stop()

    def do_drop_table(self, arg):
        """Drop a table: drop <table_name>"""
        if not self.ensure_handle():
            return False
        statement = "'drop table if exists {table}'".format(table=arg)
        return self.do_ddl(statement)

    def do_select(self, arg):
        """Execute a select query (terminate with ";" or blank line):
         select <...>
        """
        s = 'select {q}'.format(q=arg)
        return self.do_query(s)

    def do_query(self, arg):
        """Execute a query (terminate with ";" or blank line:
        query <query_string> (not quoted)
        """
        if not self.ensure_handle():
            return False
        t = Timer(self)
        arg = self.strip_semi(arg)
        qreq = QueryRequest().set_statement(arg)
        qreq.set_compartment(self._compartment)
        qreq.set_timeout(self._request_timeout_ms)

        while True:
            result = self._handle.query(qreq)
            for row in result.get_results():
                self.print_value(row)

            self._tp['readkb'] = self._tp['readkb'] + result.get_read_kb()
            self._tp['writekb'] = self._tp['writekb'] + result.get_write_kb()
            self._tp['read_units'] = (self._tp['read_units'] +
                                      result.get_read_units())
            self._tp['write_units'] = (self._tp['write_units'] +
                                       result.get_write_units())

            if qreq.is_done():
                break
        t.stop()

        return False

    def do_query_plan(self, arg):
        """Display the query plan for a query: query_plan <query_string>
        (not quoted)
        """
        if not self.ensure_handle():
            return False

        t = Timer(self)
        arg = self.strip_semi(arg)
        qreq = PrepareRequest().set_statement(arg).set_get_query_plan(True)
        qreq.set_compartment(self._compartment)
        result = self._handle.prepare(qreq)
        statement = result.get_prepared_statement()
        print(statement.get_query_plan())
        t.stop()
        # todo: driver plan?

    def do_get(self, arg):
        """Get a single row using a JSON key: get <table_name> 'key_as_json'"""
        args = shlex.split(arg)
        if len(args) != 2:
            print("Usage: get <table_name> 'key_as_json'")
            return False
        t = Timer(self)
        req = GetRequest().set_table_name(args[0]).set_key_from_json(args[1])
        req.set_compartment(self._compartment)
        req.set_timeout(self._request_timeout_ms)
        result = self._handle.get(req)
        self._tp['readkb'] = result.get_read_kb()
        self._tp['read_units'] = result.get_read_units()
        self._tp['writekb'] = 0
        self._tp['write_units'] = 0
        self._stats = req.get_retry_stats()

        self.print_value(result)
        t.stop()

    def do_put(self, arg):
        """Put a single row from JSON: put <table_name> 'row_as_json'"""
        args = shlex.split(arg)
        if len(args) != 2:
            print("Usage: put <table_name> 'row_as_json'")
            return False
        t = Timer(self)
        req = PutRequest().set_table_name(args[0]).set_value_from_json(args[1])
        req.set_compartment(self._compartment)
        result = self._handle.put(req)
        if result.get_version() is not None:
            print("Put succeeded")
        else:
            print("Put did not put a row")

        self._tp['readkb'] = result.get_read_kb()
        self._tp['writekb'] = result.get_write_kb()
        self._tp['read_units'] = result.get_read_units()
        self._tp['write_units'] = result.get_write_units()
        self._stats = req.get_retry_stats()
        t.stop()

    def do_put_if_present(self, arg):
        """Put a single row from JSON, if present: put_if_present
        <table_name> 'row_as_json'
        """
        args = shlex.split(arg)
        if len(args) != 2:
            print("Usage: put_if_present <table_name> 'row_as_json'")
            return False
        t = Timer(self)
        req = PutRequest().set_table_name(args[0]).set_value_from_json(args[1])
        req.set_compartment(self._compartment)
        req.set_option(PutOption.IF_PRESENT)
        result = self._handle.put(req)
        if result.get_version() is not None:
            print("Put if present succeeded")
        else:
            print("Put if present did not put a row, it may not be present")

        self._tp['readkb'] = result.get_read_kb()
        self._tp['writekb'] = result.get_write_kb()
        self._tp['read_units'] = result.get_read_units()
        self._tp['write_units'] = result.get_write_units()
        self._stats = req.get_retry_stats()
        t.stop()

    def do_put_if_absent(self, arg):
        """Put a single row from JSON, if absent: put_if_absent
        <table_name> 'row_as_json'
        """
        args = shlex.split(arg)
        if len(args) != 2:
            print("Usage: put_if_absent <table_name> 'row_as_json'")
            return False
        t = Timer(self)
        req = PutRequest().set_table_name(args[0]).set_value_from_json(args[1])
        req.set_compartment(self._compartment)
        req.set_option(PutOption.IF_ABSENT)
        result = self._handle.put(req)
        if result.get_version() is not None:
            print("Put if absent succeeded")
        else:
            print("Put if absent did not put a row, it may not be absent")
        self._tp['readkb'] = result.get_read_kb()
        self._tp['writekb'] = result.get_write_kb()
        self._tp['read_units'] = result.get_read_units()
        self._tp['write_units'] = result.get_write_units()
        self._stats = req.get_retry_stats()
        t.stop()

    # noinspection PyUnusedLocal
    def do_throughput(self, arg):
        """Print current throughput information based on the last operation"""
        self.print_value(self._tp)

    # noinspection PyUnusedLocal
    def do_retries(self, arg):
        """Print retry information based on the last operation"""
        self.print_retries(None)

    def do_load(self, arg):
        """Load one or more rows from a file, rows must be JSON:
        load <file> <table_name>
        """
        if not self.ensure_handle():
            return False
        t = Timer(self)
        args = arg.split()
        if len(args) != 2:
            print('Usage: load <path-to-file> <table_name>')
            return False
        req = PutRequest().set_table_name(args[1])
        req.set_compartment(self._compartment)
        count: int = 0
        #
        # TODO: deal with multiple objects with any format, no separators. May
        # require some real coding...
        #
        with open(args[0]) as f:
            for jsonObj in f:
                row = json.loads(jsonObj)
                req.set_value(row)
                self._handle.put(req)
                count += 1
        print('Loaded {num} rows to table {t}'.format(num=count, t=args[1]))
        t.stop()

    # noinspection PyUnusedLocal
    def do_bye(self, arg):
        """Exit the shell"""
        self.close()
        return True

    def do_quit(self, arg):
        """Exit the shell"""
        self.close()
        return True

    # alias exit for quit
    do_exit = do_quit

    # noinspection PyPep8Naming,PyUnusedLocal
    def do_EOF(self, arg):
        """Handle EOF (exit)"""
        self.close()
        return True

    def do_shell(self, arg):
        """Run a shell command"""
        output = os.popen(arg).read()
        print(output)
        self._last_output = output

    #
    # Override the default behavior to catch exceptions and continue the loop
    #
    def onecmd(self, line, **kwargs):
        try:
            return super().onecmd(line, **kwargs)
        except Exception as err:
            print(err)
            return False  # don't stop

    # override the default
    @staticmethod
    def emptyline(**kwargs):
        """Do nothing on empty line
        """
        # do nothing
        None

    def close(self):
        if self._handle is not None:
            self._handle.close()

    def ensure_handle(self):
        if self._handle is None:
            print("Nosql is not connected, use connect command")
            return False
        return True

    @staticmethod
    def make_limits(arg):
        limits = tuple(map(int, arg.replace(" ", "").split(',')))
        if len(limits) != 3:
            print("Limits must be of format 'read_units, write_units, size'")
            return None
        return TableLimits(limits[0], limits[1], limits[2])

    def print_value(self, arg):
        if self._json:
            if self._pretty:
                print(json.dumps(arg, indent=1, default=str))
            else:
                print(json.dumps(arg, default=str))
        else:
            if self._pretty:
                self._pprint.pprint(arg)
            else:
                print(arg)

    # string trailing ";"
    @staticmethod
    def strip_semi(arg):
        if arg[-1] == ';':
            return arg[:-1]
        return arg

    # noinspection PyUnusedLocal
    def print_retries(self, request):
        if self._stats is not None:
            print('Retries: {num}'.format(num=self._stats.get_retries()))


#
# From here down is command line handling required to initialize the shell
#

#
# todo: add flag for profile in credentials file
# todo: add flag for compartment
# todo: better usage message that describes creds or other auth for cloud
# todo: onprem secure...
#
def usage(val=None):
    print('Usage: nosql.py -s|--service <service> -e|--endpoint <endpoint> [-v]')
    print('    [-c|--creds <credentials_file>] - cloud service credentials file')
    print('    [-t|--tenant <tenant_id>] - cloud service tenant id (OCID)')
    print('    [-u|--user <user_id>] - cloud service user id (OCID)')
    print('    [-f|--fingerprint <fingerprint>] - cloud service fingerprint')
    print('    [-k|--key <private-key>] - cloud service private key file path or string')
    print('    [-p|--pass <pass_phrase for key or password>] - private key pass or user password')
    print('    service must be one of "cloud", "kvstore", or "cloudsim"')
    sys.exit(val)


def required(arg):
    print("Required argument is missing: {a}".format(a=arg))
    usage(2)


#
# A simple main() that parses the command line and passes arguments
# to the shell. See usage above for usage
#
# noinspection PyUnusedLocal
def main():
    opts = None
    args = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:e:t:u:f:k:p:c:v",
                                   ["help", "service=", "endpoint="])
    except getopt.GetoptError as err:
        print(err)
        usage(2)
    service = None
    verbose = False
    endpoint = None
    tenant_id = None
    user_id = None
    fingerprint = None
    private_key = None
    pass_phrase = None
    creds_file = None
    # TODO: add flag for compartment. -c is taken and should probably be
    # repurposed
    compartment = None
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
        elif o in ("-s", "--service"):
            service = a
        elif o in ("-e", "--endpoint"):
            endpoint = a
        elif o in ("-t", "--tenant"):
            tenant_id = a
        elif o in ("-u", "--user"):
            user_id = a
        elif o in ("-f", "--fingerprint"):
            fingerprint = a
        elif o in ("-k", "--key"):
            private_key = a
        elif o in ("-p", "--pass"):
            pass_phrase = a
        elif o in ("-c", "--creds"):
            creds_file = a
        else:
            print("Unknown option: {opt}".format(opt=o))
            usage(2)
    if service is None:
        required("service (one of: cloud, kvstore, cloudsim)")
    if endpoint is None:
        required("endpoint")

    if service not in {'cloud', 'kvstore', 'cloudsim'}:
        print("Unknown service: {s}".format(s=service))
        usage(2)

    # noinspection PyBroadException
    try:
        provider = get_provider(service, creds_file, tenant_id, user_id,
                                fingerprint, private_key, pass_phrase)

        NoSQLShell(service, provider, endpoint, compartment, verbose).cmdloop()
    except Exception:
        print(traceback.format_exc())


if __name__ == '__main__':
    main()
