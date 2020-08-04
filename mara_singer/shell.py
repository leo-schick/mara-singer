"""Command execution in bash shells"""

import time

from mara_pipelines import config
from mara_pipelines.logging import logger


def singer_run_shell_command(command: str, log_command: bool = True):
    """
    Runs a command in a bash shell and logs the output of the command in (near)real-time according to the
    singer specification: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output

    Args:
        command: The command to run
        log_command: When true, then the command itself is logged before execution

    Returns:
        Either (in order)
        - False when the exit code of the command was not 0
        - True when there was no output to stdout
        - The output to stdout, as an array of lines
    """
    import shlex, subprocess, threading

    if log_command:
        logger.log(command, format=logger.Format.ITALICS)

    process = subprocess.Popen(shlex.split(config.bash_command_string()) + ['-c', command],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               universal_newlines=True)

    # keep stdout output
    output_lines = []

    # unfortunately, only file descriptors and the system stream can be passed to
    # subprocess.Popen(..) (and not custom streams without a file handle).
    # So in order to see be able to log the output in real-time, we have to
    # query the output steams of the process from to separate threads
    class __ListenerThread(threading.Thread):
        def __init__(self, group=None, target=None, name=None, verbose=None):
            threading.Thread.__init__(self, group=group, target=target, name=name)#,
                                      #verbose=verbose)

            self.__has_error = False

        @property
        def has_error(self):
            return self.__has_error

        def run(self):
            self.__has_error = False

            for line in process.stdout:
                output_lines.append(line)
                logger.log(line, format=logger.Format.VERBATIM)

            for line in process.stderr:
                pos = line.find(' ')
                if pos == -1:
                    loglevel = 'NOTSET'
                    logmsg = line
                else:
                    loglevel = line[:pos]
                    logmsg = line[(pos+1):]

                if loglevel == 'INFO':
                    if logmsg.startswith('METRIC:'):
                        # This data could be used for showing execution statistics; see also https://github.com/singer-io/getting-started/blob/96a0f7addec517fcf5155284744c648fe4f16902/docs/SYNC_MODE.md#metric-messages
                        logger.log(logmsg, format=logger.Format.ITALICS)
                    else:
                        logger.log(logmsg, format=logger.Format.VERBATIM)

                elif loglevel in ['NOTSET','WARNING']:
                    logger.log(logmsg, format=logger.Format.VERBATIM)
                elif loglevel == 'DEBUG':
                    pass # DEBUG messages are ignored
                elif loglevel in ['ERROR','CRITICAL']:
                    self.__has_error = True
                    logger.log(logmsg, format=logger.Format.VERBATIM, is_error=True)

            return

    read_output_thread = __ListenerThread()
    read_output_thread.start()

    # wait until the process finishes
    while process.poll() is None:
        time.sleep(0.005)

    read_output_thread.join()

    if read_output_thread.has_error:
        logger.log('Singer tap error occured', is_error=True, format=logger.Format.ITALICS)
        return False

    exitcode = process.returncode
    if exitcode != 0:
        logger.log(f'exit code {exitcode}', is_error=True, format=logger.Format.ITALICS)
        return False

    return output_lines or True