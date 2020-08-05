import threading

from mara_pipelines.logging import logger

class SingerTapReadLogThread(threading.Thread):
    """
    A thread class handling read of singer log from stdout
    See also: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output

    Args:
        process: The process running the singer tap command
    """
    def __init__(self, process):
        threading.Thread.__init__(self)

        self.process = process
        self._has_error = False

    @property
    def has_error(self):
        return self._has_error

    def run(self):
        self._has_error = False

        for line in self.process.stderr:
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
                self._has_error = True
                logger.log(logmsg, format=logger.Format.VERBATIM, is_error=True)
